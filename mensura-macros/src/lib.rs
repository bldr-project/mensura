//! Procedural macros for [mensura](https://docs.rs/mensura).
//!
//! Use them through the `mensura` crate — they expand to paths rooted at
//! `::mensura`, so the crate must be reachable under that name.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Attribute, Expr, ExprLit, Ident, LitStr, Token};

/// Must match `mensura::internal::MAX_VALUES`.
const MAX_VALUES: usize = 16;

/// The field a `count!` implies.
const COUNT_FIELD: &str = "count";

/// The field a `timer!` adds when it reports.
const TIMER_FIELD: &str = "time";

/// Records a measurement.
///
/// The first argument names the measurement — what is being measured, and the
/// InfluxDB series the point lands in. After it, bare `key = value` pairs are
/// tags and a `?` prefix makes a field:
///
/// ```ignore
/// measure!("call_latency", host = "web-1", ?elapsed = start.elapsed(), ?ok = true);
/// ```
///
/// The name is stored as a `&'static str`, so it must be a string literal or a
/// `const`; it cannot be computed at runtime.
///
/// Tag and field values, by contrast, are arbitrary expressions, macro calls
/// included:
///
/// ```ignore
/// measure!("call_latency", tag = format!("foobar-{baz}"), ?rows = rows.len());
/// ```
///
/// A name on its own records the variable of that name under that key, the way
/// struct field shorthand works:
///
/// ```ignore
/// measure!("call_latency", host, region, ?elapsed);   // host = host, …
/// ```
///
/// At least one field is required — a point of tags alone has nothing to
/// record and InfluxDB rejects it. Use [`count!`] for "this happened" events.
///
/// A doc comment written *inside* the invocation is attached to the point and
/// shown by backends that support it:
///
/// ```ignore
/// measure!(
///     /// Time spent waiting for the upstream response.
///     "call_latency",
///     route = "/search",
///     ?elapsed = start.elapsed(),
/// );
/// ```
///
/// Expands to a `try_send` on the global queue, guarded by a check that a
/// backend is registered — with no backend, the arguments are not even
/// evaluated.
#[proc_macro]
pub fn measure(input: TokenStream) -> TokenStream {
    let args = syn::parse_macro_input!(input as Args);
    expand(args, Shape::Measure).into()
}

/// Records a measurement with an implied `count = 1` field.
///
/// ```ignore
/// count!("http_request", route = "/search", status = 200);
/// ```
///
/// Takes the same arguments as [`measure!`] — a measurement name first,
/// then tags and fields — but needs no field of its own. An explicit
/// `?count = …` overrides the implied one.
#[proc_macro]
pub fn count(input: TokenStream) -> TokenStream {
    let args = syn::parse_macro_input!(input as Args);
    expand(args, Shape::Count).into()
}

/// Starts a [`Timer`](https://docs.rs/mensura/latest/mensura/struct.Timer.html)
/// that records `time = <elapsed>` when it is dropped.
///
/// ```ignore
/// let _timer = timer!("query_duration", operation = "select");
/// ```
///
/// Takes the same arguments as [`measure!`] — a measurement name first,
/// then tags and fields — but needs no field of its own. They are captured up
/// front;
/// the elapsed time is added at drop. Bind the timer to a name — `let _ =
/// timer!(…)` drops it immediately and measures nothing.
#[proc_macro]
pub fn timer(input: TokenStream) -> TokenStream {
    let args = syn::parse_macro_input!(input as Args);
    expand(args, Shape::Timer).into()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    Measure,
    Count,
    Timer,
}

impl Shape {
    /// How many entries the expansion adds on top of the user's.
    fn reserved(self) -> usize {
        match self {
            Shape::Measure => 0,
            // `count!` only adds a field when the user did not write one, but
            // budgeting for it keeps the diagnostic honest in both cases.
            Shape::Count | Shape::Timer => 1,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Tag,
    Field,
}

impl Kind {
    fn path(self) -> TokenStream2 {
        match self {
            Kind::Tag => quote!(::mensura::internal::ValueType::Tag),
            Kind::Field => quote!(::mensura::internal::ValueType::Field),
        }
    }

    fn describe(self) -> &'static str {
        match self {
            Kind::Tag => "tag",
            Kind::Field => "field",
        }
    }
}

struct Entry {
    kind: Kind,
    key: String,
    key_span: Span,
    value: Expr,
}

impl Parse for Entry {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let kind = if input.peek(Token![?]) {
            input.parse::<Token![?]>()?;
            Kind::Field
        } else {
            Kind::Tag
        };

        // `parse_any` so that keys like `type` or `fn` are usable; the ident is
        // kept whole because the shorthand form reads the variable of the same
        // name, and `r#type` has to stay raw to name it.
        let (key, key_span, ident) = if input.peek(LitStr) {
            let literal: LitStr = input.parse()?;
            (literal.value(), literal.span(), None)
        } else {
            let ident = Ident::parse_any(input)?;
            (ident.unraw().to_string(), ident.span(), Some(ident))
        };

        if key.is_empty() {
            return Err(syn::Error::new(
                key_span,
                "measurement keys cannot be empty",
            ));
        }

        let value = if input.peek(Token![=]) {
            input.parse::<Token![=]>()?;
            input.parse::<Expr>()?
        } else {
            // Shorthand: `measure!(host, ?count)` records the variables `host`
            // and `count` under their own names.
            let Some(ident) = ident else {
                return Err(syn::Error::new(
                    key_span,
                    "a string-literal key needs an explicit value, as in `\"my.key\" = value`",
                ));
            };
            Expr::Path(syn::ExprPath {
                attrs: Vec::new(),
                qself: None,
                path: syn::Path::from(ident),
            })
        };

        Ok(Entry {
            kind,
            key,
            key_span,
            value,
        })
    }
}

impl Entry {
    /// The `(ValueType, key, Value)` tuple this entry expands to.
    fn to_tokens(&self) -> TokenStream2 {
        let kind = self.kind.path();
        let key = LitStr::new(&self.key, self.key_span);
        let value = self.value_tokens();
        quote!((#kind, #key, #value))
    }

    fn value_tokens(&self) -> TokenStream2 {
        // A string literal is already `&'static str`, so it can go straight
        // into the point without an allocation.
        if let Expr::Lit(ExprLit {
            lit: syn::Lit::Str(literal),
            ..
        }) = &self.value
        {
            return quote!(::mensura::internal::Value::StaticStr(#literal));
        }

        let value = &self.value;
        let span = value.span();
        match self.kind {
            Kind::Tag => {
                quote::quote_spanned!(span=> ::mensura::internal::MeasurementTag::into_tag_value(#value))
            }
            Kind::Field => {
                quote::quote_spanned!(span=> ::mensura::internal::MeasurementField::into_field_value(#value))
            }
        }
    }
}

struct Args {
    docstring: Option<String>,
    /// The measurement name: what is being measured, and the series the point
    /// is stored under.
    measurement: Expr,
    entries: Vec<Entry>,
}

impl Parse for Args {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let docstring = parse_docstring(&attrs)?;

        if input.is_empty() || input.peek(Token![?]) {
            return Err(syn::Error::new(
                Span::call_site(),
                format!("missing measurement name: {USAGE}"),
            ));
        }
        let measurement: Expr = input.parse().map_err(|error| {
            syn::Error::new(
                error.span(),
                format!("expected a measurement name: {USAGE}"),
            )
        })?;
        if input.peek(Token![=]) {
            return Err(syn::Error::new_spanned(
                &measurement,
                format!("the first argument is the measurement name, not a tag: {USAGE}"),
            ));
        }
        check_measurement(&measurement)?;

        let entries = if input.is_empty() {
            Vec::new()
        } else {
            input.parse::<Token![,]>()?;
            Punctuated::<Entry, Token![,]>::parse_terminated(input)?
                .into_iter()
                .collect::<Vec<_>>()
        };
        check_duplicates(&entries)?;

        Ok(Args {
            docstring,
            measurement,
            entries,
        })
    }
}

/// Shown whenever the first argument is missing or misread.
const USAGE: &str =
    "write it first, as in `measure!(\"call_latency\", host = \"web-1\", ?ms = 12)`";

/// The measurement is stored as a `&'static str`, so it has to be a constant.
/// A string literal is the expected form; a `const` path is accepted too, and
/// anything else is rejected here rather than as a type error inside the
/// expansion.
fn check_measurement(measurement: &Expr) -> syn::Result<()> {
    match measurement {
        Expr::Lit(ExprLit {
            lit: syn::Lit::Str(literal),
            ..
        }) => {
            if literal.value().is_empty() {
                return Err(syn::Error::new_spanned(
                    literal,
                    "the measurement name cannot be empty",
                ));
            }
            Ok(())
        }
        Expr::Path(_) => Ok(()),
        other => Err(syn::Error::new_spanned(
            other,
            format!(
                "the measurement name must be a string literal or a `const`, \
                 because it is stored as a `&'static str`: {USAGE}"
            ),
        )),
    }
}

/// Joins the `///` lines written at the start of the invocation.
fn parse_docstring(attrs: &[Attribute]) -> syn::Result<Option<String>> {
    let mut lines = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("doc") {
            return Err(syn::Error::new_spanned(
                attr,
                "only doc comments are allowed here",
            ));
        }
        let syn::Meta::NameValue(meta) = &attr.meta else {
            return Err(syn::Error::new_spanned(attr, "expected a doc comment"));
        };
        let Expr::Lit(ExprLit {
            lit: syn::Lit::Str(literal),
            ..
        }) = &meta.value
        else {
            return Err(syn::Error::new_spanned(attr, "expected a doc comment"));
        };
        lines.push(literal.value().trim().to_string());
    }

    if lines.is_empty() {
        Ok(None)
    } else {
        Ok(Some(lines.join("\n")))
    }
}

fn check_duplicates(entries: &[Entry]) -> syn::Result<()> {
    for (index, entry) in entries.iter().enumerate() {
        if let Some(earlier) = entries[..index]
            .iter()
            .find(|other| other.kind == entry.kind && other.key == entry.key)
        {
            return Err(syn::Error::new(
                entry.key_span,
                format!(
                    "duplicate {} `{}`; the earlier one would be silently overwritten",
                    earlier.kind.describe(),
                    entry.key
                ),
            ));
        }
    }
    Ok(())
}

/// Turns parsed arguments into an expansion, reporting any problem where the
/// caller wrote it.
fn expand(args: Args, shape: Shape) -> TokenStream2 {
    match build(args, shape) {
        Ok(tokens) => tokens,
        Err(error) => error.to_compile_error(),
    }
}

fn build(mut args: Args, shape: Shape) -> syn::Result<TokenStream2> {
    if matches!(shape, Shape::Count) {
        let has_count = args
            .entries
            .iter()
            .any(|entry| entry.kind == Kind::Field && entry.key == COUNT_FIELD);
        if !has_count {
            args.entries.push(Entry {
                kind: Kind::Field,
                key: COUNT_FIELD.to_string(),
                key_span: Span::call_site(),
                value: syn::parse_quote!(1i64),
            });
        }
    }

    // A point with no field is not representable: InfluxDB rejects it, so it
    // would be silently dropped downstream. `count!` and `timer!` always supply
    // one of their own.
    if matches!(shape, Shape::Measure)
        && !args.entries.iter().any(|entry| entry.kind == Kind::Field)
    {
        return Err(syn::Error::new(
            Span::call_site(),
            "a measurement needs at least one field, written with a `?` prefix as in \
             `?latency = elapsed`; tags alone cannot be recorded — use `count!` to record \
             `count = 1` against these tags",
        ));
    }

    let budget = MAX_VALUES - shape.reserved();
    if args.entries.len() > budget {
        let extra = match shape {
            Shape::Measure => String::new(),
            Shape::Count => format!(" (`count!` reserves one for `{COUNT_FIELD}`)"),
            Shape::Timer => format!(" (`timer!` reserves one for `{TIMER_FIELD}`)"),
        };
        let message = format!(
            "a measurement carries at most {budget} tags and fields{extra}, got {}",
            args.entries.len()
        );
        return Err(syn::Error::new(Span::call_site(), message));
    }

    let measurement = &args.measurement;
    let docstring = match &args.docstring {
        Some(doc) => quote!(::core::option::Option::Some(#doc)),
        None => quote!(::core::option::Option::None),
    };
    let entries = args.entries.iter().map(Entry::to_tokens);
    // The array type is spelled out: `array_vec!` tries to parse a leading `ty`
    // fragment, and a bare list of tuple expressions trips that parse up.
    let values = quote!(::mensura::internal::tinyvec::array_vec![
        [::mensura::internal::Entry; ::mensura::internal::MAX_VALUES] => #(#entries),*
    ]);

    // `#[allow(unused_braces)]`: a braced value expression, `?n = { … }`, ends
    // up as a block passed to a function. That is the caller's business.
    Ok(match shape {
        Shape::Measure | Shape::Count => quote! {
            {
                // Skip the whole point — including evaluating the values — when
                // no backend is listening.
                if let ::core::option::Option::Some(mensura_queue) =
                    &*::mensura::internal::GLOBAL_QUEUE.get()
                {
                    #[allow(unused_braces)]
                    mensura_queue.send(::mensura::internal::DataPoint {
                        measurement: #measurement,
                        timestamp: ::std::time::Instant::now(),
                        docstring: #docstring,
                        values: #values,
                    });
                }
            }
        },
        // A timer has to capture its values eagerly: a backend may well be
        // registered by the time it is dropped.
        Shape::Timer => quote! {
            {
                #[allow(unused_braces)]
                let mensura_timer =
                    ::mensura::internal::Timer::new(#measurement, #docstring, #values);
                mensura_timer
            }
        },
    })
}
