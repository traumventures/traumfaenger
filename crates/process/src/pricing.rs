//! List prices for providers that do not report what they charge.
//!
//! OpenRouter reports the charged amount per call (`usage.cost`), which then
//! wins. Langdock and direct provider APIs report only tokens; their cost is
//! computed here and marked `priced`. Rates are USD per million tokens. A
//! model without an entry gets no cost rather than a guessed one.

use crate::Usage;

pub struct Price {
    /// Matched against the normalized served model, see [`normalize`].
    pub model: &'static str,
    pub input: f64,
    pub cached: f64,
    pub cache_write: f64,
    pub output: f64,
}

/// Longest prefixes first where they overlap.
pub const PRICES: &[Price] = &[
    // Langdock's Anthropic endpoint (support agent). Anthropic list prices;
    // cache reads 0.1x, 5-minute cache writes 1.25x of input.
    Price { model: "claude-sonnet-4-6", input: 3.0, cached: 0.3, cache_write: 3.75, output: 15.0 },
    Price { model: "claude-haiku-4-5", input: 1.0, cached: 0.1, cache_write: 1.25, output: 5.0 },
    // Langdock's Google endpoint. Google list price as of 2026-07, as in
    // traumwohnung's model-pricing.ts; Langdock may add a markup.
    Price { model: "gemini-3-5-flash", input: 1.5, cached: 0.375, cache_write: 1.5, output: 9.0 },
];

/// `anthropic/claude-sonnet-4.6`, `claude-sonnet-4-6-default` and
/// `claude-sonnet-4-6` all become `claude-sonnet-4-6`.
pub fn normalize(model: &str) -> String {
    let name = model.rsplit('/').next().unwrap_or(model).to_ascii_lowercase().replace('.', "-");
    name.strip_suffix("-default").map(str::to_owned).unwrap_or(name)
}

pub fn find(model: &str) -> Option<&'static Price> {
    let normalized = normalize(model);
    PRICES.iter().find(|price| normalized.starts_with(price.model))
}

/// Cost of a call from its usage, or `None` without a price or token counts.
pub fn price(model: &str, usage: &Usage) -> Option<f64> {
    let price = find(model)?;
    let input = usage.input?;
    let output = usage.output.unwrap_or(0);
    let cached = usage.cached.unwrap_or(0);
    let written = usage.cache_write.unwrap_or(0);
    let uncached = input.saturating_sub(cached + written);
    let per_token = |tokens: u64, rate: f64| tokens as f64 * rate / 1_000_000.0;
    Some(
        per_token(uncached, price.input)
            + per_token(cached, price.cached)
            + per_token(written, price.cache_write)
            + per_token(output, price.output),
    )
}
