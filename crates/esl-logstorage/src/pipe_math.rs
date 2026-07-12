//! Port of `lib/logstorage/pipe_math.go` — the `| math ...` (aka `| eval ...`)
//! pipe: per-row arithmetic expression evaluation producing new fields.
//!
//! PORT NOTE — parser deferred: Go's `parsePipeMath`/`parseMathExpr`/… drive the
//! shared `lexer` type (`lex.isKeyword`, `lex.nextCompoundMathToken`, …), which
//! is not ported yet. The lexer-driven parse is therefore omitted; instead the
//! `MathExpr`/`MathEntry`/`PipeMath` constructors are exposed `pub(crate)` so a
//! future parser (and the tests here) can build the expression tree directly.
//! The self-contained pieces — the expression tree, `String()` rendering,
//! operator-priority table, the full evaluator and every `mathFunc` — are ported.
//!
//! PORT NOTE — `try_parse_number`: Go's `tryParseNumber` lives in
//! `block_result.go`; private copies were homed in `stats_histogram.rs` and
//! `filter_range.rs` (as noted there). It is exposed here `pub(crate)` per the
//! porting task; those copies should later be replaced by a `use` of this one
//! (they are out of scope for this change).
//!
//! PORT NOTE — per-worker scratch: Go pools scratch buffers per worker via
//! `atomicutil.Slice[pipeMathProcessorShard]`. Here `write_block` allocates its
//! scratch per call (no cross-block state exists for `math`), which is simpler
//! and behaviorally identical.
//!
//! PORT NOTE — dead_code: the `pub(crate)` `PipeMath`/`MathEntry`/`MathExpr`
//! constructors are the surface the deferred parser (and query planner) will
//! call. Until that lexer-driven registration lands they have no non-test
//! caller, so the module allows `dead_code`; drop the allow once wired.
#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU32, Ordering};

use esl_common::{decimal, encoding};

use crate::block_result::{BlockResult, ColRef, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::stream_filter::quote_token_if_needed;
use crate::values_encoder::{
    try_parse_bytes, try_parse_duration, try_parse_float64, try_parse_ipv4,
    try_parse_timestamp_rfc3339_nano,
};

const NAN: f64 = f64::NAN;

/// `| math ...` pipe: a list of `expr as resultField` entries.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PipeMath {
    pub(crate) entries: Vec<MathEntry>,
}

/// A single `expr as resultField` clause.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MathEntry {
    /// The calculated expr result is stored in `result_field`.
    pub(crate) result_field: String,
    /// The expression to calculate.
    pub(crate) expr: MathExpr,
}

/// A node in the math expression tree.
///
/// PORT NOTE: Go stores the `mathFunc` function pointer `f` on the node; here we
/// dispatch by `op` at evaluation time (see [`apply_math_func`]), which keeps
/// the node cleanly `Clone`/`PartialEq`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MathExpr {
    /// If set, the expr returns `const_value`.
    is_const: bool,
    const_value: f64,
    /// Original string representation of `const_value` (used by `to_string`).
    const_value_str: String,

    /// If non-empty, the expr fetches numeric values from this field.
    field_name: String,

    /// Args for the expr.
    args: Vec<MathExpr>,

    /// Operation / function name.
    op: String,

    /// Whether the expr was wrapped in parens (affects operator balancing).
    wrapped_in_parens: bool,
}

impl PipeMath {
    pub(crate) fn new(entries: Vec<MathEntry>) -> Self {
        Self { entries }
    }
}

impl MathEntry {
    pub(crate) fn new(result_field: impl Into<String>, expr: MathExpr) -> Self {
        Self {
            result_field: result_field.into(),
            expr,
        }
    }
}

// PORT NOTE: Go's `bySortField`-style `String()` methods become `Display`
// impls; `.to_string()` still works via the blanket `ToString` impl (avoids
// clippy::inherent_to_string).
impl std::fmt::Display for MathEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = self.expr.to_string();
        if is_math_binary_op(&self.expr.op) {
            s = format!("({s})");
        }
        s += " as ";
        s += &quote_token_if_needed(&self.result_field);
        f.write_str(&s)
    }
}

// PORT NOTE: Go's `mathExpr.String()` becomes a `Display` impl; `.to_string()`
// still works via the blanket `ToString` impl (avoids clippy::inherent_to_string).
impl std::fmt::Display for MathExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_const {
            return f.write_str(&self.const_value_str);
        }
        if !self.field_name.is_empty() {
            return f.write_str(&quote_token_if_needed(&self.field_name));
        }

        if is_math_binary_op(&self.op) {
            let op_priority = get_math_binary_op_priority(&self.op);
            let left = &self.args[0];
            let right = &self.args[1];
            let mut left_str = left.to_string();
            let mut right_str = right.to_string();
            if is_math_binary_op(&left.op) && get_math_binary_op_priority(&left.op) > op_priority {
                left_str = format!("({left_str})");
            }
            if is_math_binary_op(&right.op) && get_math_binary_op_priority(&right.op) >= op_priority
            {
                right_str = format!("({right_str})");
            }
            return write!(f, "{left_str} {} {right_str}", self.op);
        }

        if self.op == "unary_minus" {
            let mut arg_str = self.args[0].to_string();
            if is_math_binary_op(&self.args[0].op) {
                arg_str = format!("({arg_str})");
            }
            return write!(f, "-{arg_str}");
        }

        let a: Vec<String> = self.args.iter().map(|arg| arg.to_string()).collect();
        write!(f, "{}({})", self.op, a.join(", "))
    }
}

impl MathExpr {
    /// Builds a constant expr; `str_repr` is the original textual form.
    pub(crate) fn new_const(value: f64, str_repr: impl Into<String>) -> Self {
        Self {
            is_const: true,
            const_value: value,
            const_value_str: str_repr.into(),
            field_name: String::new(),
            args: Vec::new(),
            op: String::new(),
            wrapped_in_parens: false,
        }
    }

    /// Builds a field-reference expr.
    pub(crate) fn new_field(name: impl Into<String>) -> Self {
        Self {
            is_const: false,
            const_value: 0.0,
            const_value_str: String::new(),
            field_name: name.into(),
            args: Vec::new(),
            op: String::new(),
            wrapped_in_parens: false,
        }
    }

    /// Builds a binary-op expr (`op` one of the math binary ops).
    pub(crate) fn new_binary(op: impl Into<String>, left: MathExpr, right: MathExpr) -> Self {
        Self {
            is_const: false,
            const_value: 0.0,
            const_value_str: String::new(),
            field_name: String::new(),
            args: vec![left, right],
            op: op.into(),
            wrapped_in_parens: false,
        }
    }

    /// Builds a function-call expr (`abs`, `min`, `round`, `now`, …).
    pub(crate) fn new_func(op: impl Into<String>, args: Vec<MathExpr>) -> Self {
        Self {
            is_const: false,
            const_value: 0.0,
            const_value_str: String::new(),
            field_name: String::new(),
            args,
            op: op.into(),
            wrapped_in_parens: false,
        }
    }

    /// Builds a `unary_minus` expr.
    pub(crate) fn new_unary_minus(arg: MathExpr) -> Self {
        Self::new_func("unary_minus", vec![arg])
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        if self.is_const {
            return;
        }
        if !self.field_name.is_empty() {
            pf.add_allow_filter(&self.field_name);
            return;
        }
        for arg in &self.args {
            arg.update_needed_fields(pf);
        }
    }
}

/// Returns the priority of a binary op, or `None` if `op` is not a binary op.
fn math_binary_op_priority(op: &str) -> Option<i32> {
    match op {
        "^" => Some(1),
        "*" | "/" | "%" => Some(2),
        "+" | "-" => Some(3),
        "&" => Some(4),
        "xor" => Some(5),
        "or" => Some(6),
        "default" => Some(10),
        _ => None,
    }
}

fn is_math_binary_op(op: &str) -> bool {
    math_binary_op_priority(op).is_some()
}

fn get_math_binary_op_priority(op: &str) -> i32 {
    match math_binary_op_priority(op) {
        Some(p) => p,
        None => {
            esl_common::panicf!("BUG: unexpected binary op: {op:?}");
            unreachable!()
        }
    }
}

impl std::fmt::Display for PipeMath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let a: Vec<String> = self.entries.iter().map(|e| e.to_string()).collect();
        write!(f, "math {}", a.join(", "))
    }
}

impl Pipe for PipeMath {
    fn to_string(&self) -> String {
        format!("{self}")
    }

    fn stats_labels_tail_op(&self) -> Option<crate::pipe::StatsTailOp> {
        // The math pipe adds additional metrics to the given set of fields.
        Some(crate::pipe::StatsTailOp::Math {
            result_fields: self
                .entries
                .iter()
                .map(|e| e.result_field.clone())
                .collect(),
        })
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        // PORT NOTE: mirrors Go — math may clobber _time, but Go still returns
        // true here (with a TODO). Keep parity.
        true
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        for e in self.entries.iter().rev() {
            if pf.match_string(&e.result_field) {
                pf.add_deny_filter(&e.result_field);
                e.expr.update_needed_fields(pf);
            }
        }
    }

    fn new_pipe_processor(
        &self,
        _concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        Arc::new(PipeMathProcessor {
            entries: self.entries.clone(),
            pp_next,
        })
    }
}

struct PipeMathProcessor {
    entries: Vec<MathEntry>,
    pp_next: Arc<dyn PipeProcessor>,
}

impl PipeProcessor for PipeMathProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        for e in &self.entries {
            let (values, min_value, max_value) = execute_math_entry(e, br);
            let rc = ResultColumn {
                name: e.result_field.clone(),
                values,
            };
            br.add_result_column_float64(rc, min_value, max_value);
        }

        self.pp_next.write_block(worker_id, br);
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

/// Evaluates one entry, returning the marshaled per-row values plus min/max.
fn execute_math_entry(e: &MathEntry, br: &mut BlockResult) -> (Vec<Vec<u8>>, f64, f64) {
    let rows_len = br.rows_len();
    let r = execute_expr(&e.expr, br, rows_len);

    let mut values: Vec<Vec<u8>> = Vec::with_capacity(rows_len);
    let mut min_value = NAN;
    let mut max_value = NAN;
    for &f in &r {
        if min_value.is_nan() {
            min_value = f;
            max_value = f;
        } else if f < min_value {
            min_value = f;
        } else if f > max_value {
            max_value = f;
        }
        // PORT NOTE: Go's `marshalFloat64` stores the 8-byte big-endian bit
        // pattern (`encoding.MarshalUint64(Float64bits(f))`), which the FLOAT64
        // column read path decodes back via `unmarshal_float64`.
        let mut b = Vec::new();
        encoding::marshal_uint64(&mut b, f.to_bits());
        values.push(b);
    }

    (values, min_value, max_value)
}

/// Recursively evaluates `me` into a fresh `Vec<f64>` of length `rows_len`.
fn execute_expr(me: &MathExpr, br: &mut BlockResult, rows_len: usize) -> Vec<f64> {
    if me.is_const {
        return vec![me.const_value; rows_len];
    }
    if !me.field_name.is_empty() {
        let c = br.get_column_by_name(&me.field_name);
        return load_arg_values_from_column(br, c, rows_len);
    }

    let args: Vec<Vec<f64>> = me
        .args
        .iter()
        .map(|arg| execute_expr(arg, br, rows_len))
        .collect();
    let mut result = vec![0.0f64; rows_len];
    apply_math_func(&me.op, &mut result, &args);
    result
}

/// Loads numeric values from a column.
///
/// PORT NOTE: Go's `loadArgValuesFromColumn` has per-`valueType` fast paths that
/// read encoded values directly. Here we uniformly decode to strings and apply
/// `parse_math_number`; the decoded string round-trips to the identical number
/// for every numeric value type, so results match while avoiding the private
/// column encoding accessors.
fn load_arg_values_from_column(br: &mut BlockResult, c: ColRef, rows_len: usize) -> Vec<f64> {
    if br.column_is_time(c) {
        return br.get_timestamps().iter().map(|&ts| ts as f64).collect();
    }

    let values = br.column_get_values(c);
    let mut dst = Vec::with_capacity(rows_len);
    let mut f = NAN;
    let mut prev: Option<&[u8]> = None;
    for v in values {
        if prev != Some(v.as_slice()) {
            f = parse_math_number(&String::from_utf8_lossy(v));
        }
        dst.push(f);
        prev = Some(v.as_slice());
    }
    dst
}

// ---------------------------------------------------------------------------
// math functions (dispatched by op name)
// ---------------------------------------------------------------------------

fn apply_math_func(op: &str, result: &mut [f64], args: &[Vec<f64>]) {
    match op {
        "&" => bitwise(result, args, |a, b| a & b),
        "or" => bitwise(result, args, |a, b| a | b),
        "xor" => bitwise(result, args, |a, b| a ^ b),
        "+" => binary(result, args, |a, b| a + b),
        "-" => binary(result, args, |a, b| a - b),
        "*" => binary(result, args, |a, b| a * b),
        "/" => binary(result, args, |a, b| a / b),
        "%" => math_func_mod(result, args),
        "^" => binary(result, args, f64::powf),
        "default" => math_func_default(result, args),
        "abs" => unary(result, args, f64::abs),
        "exp" => unary(result, args, f64::exp),
        "ln" => unary(result, args, f64::ln),
        "unary_minus" => unary(result, args, |x| -x),
        "ceil" => unary(result, args, f64::ceil),
        "floor" => unary(result, args, f64::floor),
        "max" => math_func_max(result, args),
        "min" => math_func_min(result, args),
        "round" => math_func_round(result, args),
        "rand" => math_func_rand(result),
        "now" => math_func_now(result),
        _ => esl_common::panicf!("BUG: unexpected math op: {op:?}"),
    }
}

fn binary(result: &mut [f64], args: &[Vec<f64>], f: impl Fn(f64, f64) -> f64) {
    let a = &args[0];
    let b = &args[1];
    for i in 0..result.len() {
        result[i] = f(a[i], b[i]);
    }
}

fn unary(result: &mut [f64], args: &[Vec<f64>], f: impl Fn(f64) -> f64) {
    let a = &args[0];
    for i in 0..result.len() {
        result[i] = f(a[i]);
    }
}

fn bitwise(result: &mut [f64], args: &[Vec<f64>], f: impl Fn(u64, u64) -> u64) {
    let a = &args[0];
    let b = &args[1];
    for i in 0..result.len() {
        if a[i].is_nan() || b[i].is_nan() {
            result[i] = NAN;
        } else {
            result[i] = f(a[i] as u64, b[i] as u64) as f64;
        }
    }
}

fn math_func_mod(result: &mut [f64], args: &[Vec<f64>]) {
    let a = &args[0];
    let b = &args[1];
    for i in 0..result.len() {
        let x = a[i];
        let y = b[i];
        let x_int = x as i64;
        let y_int = y as i64;
        if x_int as f64 == x && y_int as f64 == y {
            // Fast path - integer modulo.
            if y_int == 0 {
                result[i] = NAN;
            } else {
                result[i] = (x_int % y_int) as f64;
            }
        } else {
            // Slow path - floating point modulo.
            result[i] = x % y;
        }
    }
}

fn math_func_default(result: &mut [f64], args: &[Vec<f64>]) {
    let values = &args[0];
    let default_values = &args[1];
    for i in 0..result.len() {
        let f = values[i];
        result[i] = if f.is_nan() { default_values[i] } else { f };
    }
}

fn math_func_max(result: &mut [f64], args: &[Vec<f64>]) {
    for i in 0..result.len() {
        let mut f = NAN;
        for arg in args {
            if f.is_nan() || arg[i] > f {
                f = arg[i];
            }
        }
        result[i] = f;
    }
}

fn math_func_min(result: &mut [f64], args: &[Vec<f64>]) {
    for i in 0..result.len() {
        let mut f = NAN;
        for arg in args {
            if f.is_nan() || arg[i] < f {
                f = arg[i];
            }
        }
        result[i] = f;
    }
}

fn math_func_round(result: &mut [f64], args: &[Vec<f64>]) {
    let arg = &args[0];
    if args.len() == 1 {
        // Round to integer.
        for i in 0..result.len() {
            result[i] = arg[i].round();
        }
        return;
    }

    // Round to nearest.
    let nearest = &args[1];
    let mut f = 0.0;
    for i in 0..result.len() {
        if i == 0 || arg[i - 1] != arg[i] || nearest[i - 1] != nearest[i] {
            f = round(arg[i], nearest[i]);
        }
        result[i] = f;
    }
}

fn round(f: f64, nearest: f64) -> f64 {
    let (_, e) = decimal::from_float(nearest);
    let p10 = 10f64.powi(-(e as i32));
    let mut f = f + 0.5 * nearest.copysign(f);
    f -= f % nearest;
    let f = (f * p10).trunc();
    f / p10
}

// PORT NOTE: Go's `mathFuncRand` uses `fastrand.Uint32`. Replaced with a small
// per-process xorshift PRNG seeded from the clock — `rand()` is non-deterministic
// by design, so exact bit-parity is not required.
static RAND_STATE: AtomicU32 = AtomicU32::new(0);

fn next_rand_u32() -> u32 {
    let mut x = RAND_STATE.load(Ordering::Relaxed);
    if x == 0 {
        x = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0x9e37_79b9)
            | 1;
    }
    // xorshift32
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    RAND_STATE.store(x, Ordering::Relaxed);
    x
}

fn math_func_rand(result: &mut [f64]) {
    for r in result.iter_mut() {
        let n = next_rand_u32();
        *r = n as f64 / (1u64 << 32) as f64;
    }
}

fn math_func_now(result: &mut [f64]) {
    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as f64)
        .unwrap_or(0.0);
    for r in result.iter_mut() {
        *r = now_nanos;
    }
}

// ---------------------------------------------------------------------------
// number parsing (ported from Go's parseMathNumber / tryParseNumber)
// ---------------------------------------------------------------------------

/// Port of Go's `parseMathNumber` (pipe_math.go).
pub(crate) fn parse_math_number(s: &str) -> f64 {
    if let Some(f) = try_parse_number(s) {
        return f;
    }
    if let Some(nsecs) = try_parse_timestamp_rfc3339_nano(s) {
        return nsecs as f64;
    }
    if let Some(ip) = try_parse_ipv4(s) {
        return ip as f64;
    }
    NAN
}

/// Port of Go's `tryParseNumber` (block_result.go).
pub(crate) fn try_parse_number(s: &str) -> Option<f64> {
    if s.is_empty() {
        return None;
    }
    if let Some(f) = try_parse_float64(s) {
        return Some(f);
    }
    if let Some(nsecs) = try_parse_duration(s) {
        return Some(nsecs as f64);
    }
    if let Some(bytes) = try_parse_bytes(s) {
        return Some(bytes as f64);
    }
    if is_likely_number(s) {
        if let Ok(f) = s.parse::<f64>() {
            return Some(f);
        }
        if let Some(n) = parse_int_go(s) {
            return Some(n as f64);
        }
    }
    None
}

fn is_likely_number(s: &str) -> bool {
    if !is_number_prefix(s) {
        return false;
    }
    if s.matches('.').count() > 1 {
        // This is likely an IP address.
        return false;
    }
    if s.contains(':') || s.matches('-').count() > 2 {
        // This is likely a timestamp.
        return false;
    }
    true
}

/// Port of Go's `isNumberPrefix` (parser.go).
fn is_number_prefix(s: &str) -> bool {
    let mut b = s.as_bytes();
    if b.is_empty() {
        return false;
    }
    if b[0] == b'-' || b[0] == b'+' {
        b = &b[1..];
        if b.is_empty() {
            return false;
        }
    }
    if b.len() >= 3 && s[s.len() - b.len()..].eq_ignore_ascii_case("inf") {
        return true;
    }
    if b[0] == b'.' {
        b = &b[1..];
        if b.is_empty() {
            return false;
        }
    }
    b[0].is_ascii_digit()
}

/// Mirrors Go's `strconv.ParseInt(s, 0, 64)` base detection.
fn parse_int_go(s: &str) -> Option<i64> {
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (radix, digits) =
        if let Some(h) = body.strip_prefix("0x").or_else(|| body.strip_prefix("0X")) {
            (16, h)
        } else if let Some(o) = body.strip_prefix("0o").or_else(|| body.strip_prefix("0O")) {
            (8, o)
        } else if let Some(bin) = body.strip_prefix("0b").or_else(|| body.strip_prefix("0B")) {
            (2, bin)
        } else {
            (10, body)
        };
    let digits = digits.replace('_', "");
    let n = i64::from_str_radix(&digits, radix).ok()?;
    Some(if neg { -n } else { n })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::Field;
    use std::sync::Mutex;

    // PORT NOTE: Go's TestParsePipeMathSuccess/Failure and the lexer-driven
    // `expectPipeResults` harness in TestPipeMath cannot be ported until the
    // shared query lexer lands. The evaluation cases below rebuild the same
    // expression trees directly and assert the same expected outputs; the
    // String()/needed-fields/number-parsing logic is covered directly.

    struct CapturePp {
        rows: Mutex<Vec<Vec<(String, String)>>>,
    }

    impl PipeProcessor for CapturePp {
        fn write_block(&self, _worker_id: usize, br: &mut BlockResult) {
            let cols = br.get_columns();
            let names: Vec<String> = cols
                .iter()
                .map(|&c| br.column_name(c).to_string())
                .collect();
            let n = br.rows_len();
            let mut out = self.rows.lock().unwrap();
            for r in 0..n {
                let mut row = Vec::with_capacity(cols.len());
                for (i, &c) in cols.iter().enumerate() {
                    let v = br.column_get_value_at_row(c, r).to_string();
                    row.push((names[i].clone(), v));
                }
                out.push(row);
            }
        }
        fn flush(&self) -> Result<(), String> {
            Ok(())
        }
    }

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn run(pm: &PipeMath, rows: &[Vec<Field>]) -> Vec<Vec<(String, String)>> {
        let capture = Arc::new(CapturePp {
            rows: Mutex::new(Vec::new()),
        });
        let stop = Arc::new(AtomicBool::new(false));
        let pp: Arc<dyn PipeProcessor> = capture.clone();
        let proc = pm.new_pipe_processor(1, stop, pp);

        let mut br = BlockResult::default();
        br.must_init_from_rows(rows);
        proc.write_block(0, &mut br);
        proc.flush().unwrap();

        capture.rows.lock().unwrap().clone()
    }

    fn get(row: &[(String, String)], name: &str) -> Option<String> {
        row.iter().find(|(n, _)| n == name).map(|(_, v)| v.clone())
    }

    #[test]
    fn test_math_expr_string() {
        let expr = MathExpr::new_binary(
            "+",
            MathExpr::new_binary("/", MathExpr::new_field("foo"), MathExpr::new_field("bar")),
            MathExpr::new_field("baz"),
        );
        assert_eq!(expr.to_string(), "foo / bar + baz");

        let entry = MathEntry::new("a", expr);
        assert_eq!(entry.to_string(), "(foo / bar + baz) as a");

        let unary = MathExpr::new_unary_minus(MathExpr::new_field("x"));
        assert_eq!(unary.to_string(), "-x");

        let func = MathExpr::new_func(
            "min",
            vec![MathExpr::new_const(3.0, "3"), MathExpr::new_field("foo")],
        );
        assert_eq!(func.to_string(), "min(3, foo)");
    }

    #[test]
    fn test_pipe_math_field_alias() {
        // math b as a
        let pm = PipeMath::new(vec![MathEntry::new("a", MathExpr::new_field("b"))]);
        let out = run(
            &pm,
            &[vec![field("a", "v1"), field("b", "2"), field("c", "3")]],
        );
        assert_eq!(out.len(), 1);
        assert_eq!(get(&out[0], "a").as_deref(), Some("2"));
        assert_eq!(get(&out[0], "b").as_deref(), Some("2"));
        assert_eq!(get(&out[0], "c").as_deref(), Some("3"));
    }

    #[test]
    fn test_pipe_math_self_reference_nan() {
        // math a as a — 'a' holds "v1" (non-numeric) so result is NaN.
        let pm = PipeMath::new(vec![MathEntry::new("a", MathExpr::new_field("a"))]);
        let out = run(&pm, &[vec![field("a", "v1"), field("b", "2")]]);
        assert_eq!(get(&out[0], "a").as_deref(), Some("NaN"));
    }

    #[test]
    fn test_pipe_math_arith() {
        // math 2*c + b as x  =>  8
        let expr = MathExpr::new_binary(
            "+",
            MathExpr::new_binary("*", MathExpr::new_const(2.0, "2"), MathExpr::new_field("c")),
            MathExpr::new_field("b"),
        );
        let pm = PipeMath::new(vec![MathEntry::new("x", expr)]);
        let out = run(
            &pm,
            &[vec![field("a", "v1"), field("b", "2"), field("c", "3")]],
        );
        assert_eq!(get(&out[0], "x").as_deref(), Some("8"));
    }

    #[test]
    fn test_pipe_math_chained_entries() {
        // eval b+1 as a, a*2 as b, b-10.5+c as c
        let e1 = MathEntry::new(
            "a",
            MathExpr::new_binary("+", MathExpr::new_field("b"), MathExpr::new_const(1.0, "1")),
        );
        let e2 = MathEntry::new(
            "b",
            MathExpr::new_binary("*", MathExpr::new_field("a"), MathExpr::new_const(2.0, "2")),
        );
        let e3 = MathEntry::new(
            "c",
            MathExpr::new_binary(
                "+",
                MathExpr::new_binary(
                    "-",
                    MathExpr::new_field("b"),
                    MathExpr::new_const(10.5, "10.5"),
                ),
                MathExpr::new_field("c"),
            ),
        );
        let pm = PipeMath::new(vec![e1, e2, e3]);
        let out = run(
            &pm,
            &[vec![field("a", "v1"), field("b", "2"), field("c", "3")]],
        );
        assert_eq!(get(&out[0], "a").as_deref(), Some("3"));
        assert_eq!(get(&out[0], "b").as_deref(), Some("6"));
        assert_eq!(get(&out[0], "c").as_deref(), Some("-1.5"));
    }

    #[test]
    fn test_pipe_math_default_and_mod() {
        // math a / b default c
        let expr = MathExpr::new_binary(
            "default",
            MathExpr::new_binary("/", MathExpr::new_field("a"), MathExpr::new_field("b")),
            MathExpr::new_field("c"),
        );
        let pm = PipeMath::new(vec![MathEntry::new("a / b default c", expr)]);
        let out = run(
            &pm,
            &[
                vec![field("a", "v1"), field("b", "2"), field("c", "3")],
                vec![field("a", "3"), field("b", "2")],
                vec![field("a", "3"), field("b", "foo")],
            ],
        );
        assert_eq!(get(&out[0], "a / b default c").as_deref(), Some("3"));
        assert_eq!(get(&out[1], "a / b default c").as_deref(), Some("1.5"));
        assert_eq!(get(&out[2], "a / b default c").as_deref(), Some("NaN"));

        // math 5 % 0 as x => NaN
        let pm2 = PipeMath::new(vec![MathEntry::new(
            "x",
            MathExpr::new_binary(
                "%",
                MathExpr::new_const(5.0, "5"),
                MathExpr::new_const(0.0, "0"),
            ),
        )]);
        let out2 = run(&pm2, &[vec![field("foo", "bar")]]);
        assert_eq!(get(&out2[0], "x").as_deref(), Some("NaN"));
    }

    #[test]
    fn test_pipe_math_round_nearest() {
        // math round(exp(a), 0.01) for a in {0,1,2,3}
        let expr = MathExpr::new_func(
            "round",
            vec![
                MathExpr::new_func("exp", vec![MathExpr::new_field("a")]),
                MathExpr::new_const(0.01, "0.01"),
            ],
        );
        let pm = PipeMath::new(vec![MathEntry::new("r", expr)]);
        let out = run(
            &pm,
            &[
                vec![field("a", "0")],
                vec![field("a", "1")],
                vec![field("a", "2")],
                vec![field("a", "3")],
            ],
        );
        assert_eq!(get(&out[0], "r").as_deref(), Some("1"));
        assert_eq!(get(&out[1], "r").as_deref(), Some("2.72"));
        assert_eq!(get(&out[2], "r").as_deref(), Some("7.39"));
        assert_eq!(get(&out[3], "r").as_deref(), Some("20.09"));
    }

    #[test]
    fn test_update_needed_fields() {
        // math (x + 1) as y ; allow "*" => allow "*" deny "y", x still needed.
        let pm = PipeMath::new(vec![MathEntry::new(
            "y",
            MathExpr::new_binary("+", MathExpr::new_field("x"), MathExpr::new_const(1.0, "1")),
        )]);

        let mut pf = prefix_filter::Filter::default();
        pf.add_allow_filter("*");
        pm.update_needed_fields(&mut pf);
        assert!(pf.match_string("x"));
        assert!(!pf.match_string("y"));
    }

    #[test]
    fn test_try_parse_number() {
        assert_eq!(try_parse_number("123"), Some(123.0));
        assert_eq!(try_parse_number("1.5"), Some(1.5));
        assert_eq!(try_parse_number(""), None);
        assert_eq!(try_parse_number("v1"), None);
        // Bytes suffix parses into the numeric value.
        assert_eq!(try_parse_number("1KB"), Some(1000.0));

        // parse_math_number falls back to IPv4 and yields NaN for junk.
        let ip = try_parse_ipv4("123.45.67.89").unwrap() as f64;
        assert_eq!(parse_math_number("123.45.67.89"), ip);
        assert!(parse_math_number("v1").is_nan());
    }
}
