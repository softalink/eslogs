//! Port of the parts of Go's `regexp/syntax` AST used by `regexutil`:
//! the `Regexp` tree, `Equal`, `Simplify` and the `String` serializer
//! (including the Go 1.21+ flag-hoisting logic in `calcFlags`).

use std::collections::HashMap;
use std::fmt;
use std::fmt::Write as _;

use super::gofold::{MAX_FOLD, MIN_FOLD, simple_fold};

pub const MAX_RUNE: i32 = 0x10FFFF;

// Flags. Values match Go's syntax.Flags bit order.
pub const FOLD_CASE: u16 = 1 << 0; // case-insensitive match
pub const LITERAL: u16 = 1 << 1; // treat pattern as literal string
pub const CLASS_NL: u16 = 1 << 2; // allow char classes like [^a-z] and [[:space:]] to match newline
pub const DOT_NL: u16 = 1 << 3; // allow . to match newline
pub const ONE_LINE: u16 = 1 << 4; // treat ^ and $ as only matching at beginning and end of text
pub const NON_GREEDY: u16 = 1 << 5; // make repetition operators default to non-greedy
pub const PERL_X: u16 = 1 << 6; // allow Perl extensions
pub const UNICODE_GROUPS: u16 = 1 << 7; // allow \p{Han}, \P{Han} for Unicode group and negation
pub const WAS_DOLLAR: u16 = 1 << 8; // regexp OpEndText was $, not \z

pub const PERL: u16 = CLASS_NL | ONE_LINE | PERL_X | UNICODE_GROUPS;

/// A single regular expression operator. Values and ordering match Go's
/// `syntax.Op` (operators are listed in precedence order, and char class
/// operators simplest to most complex).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Op {
    NoMatch = 1,
    EmptyMatch,
    Literal,
    CharClass,
    AnyCharNotNL,
    AnyChar,
    BeginLine,
    EndLine,
    BeginText,
    EndText,
    WordBoundary,
    NoWordBoundary,
    Capture,
    Star,
    Plus,
    Quest,
    Repeat,
    Concat,
    Alternate,
    /// PORT NOTE: not a Go op. An opaque `\p{...}`/`\P{...}` Unicode class
    /// token (or a whole `[...]` char class containing one), kept unresolved
    /// because the Go `unicode` tables are not ported; the raw source text
    /// lives in `Regexp::name` and is resolved by the regex crate when the
    /// matcher is compiled. See `Parser::parse_unicode_class_token`.
    UnicodeClass = 20,
    // Pseudo-ops for the parsing stack (Go `opPseudo` starts at 128).
    PseudoLeftParen = 128,
    PseudoVerticalBar = 129,
}

pub const OP_PSEUDO: u8 = 128;

/// A node in a regular expression syntax tree.
#[derive(Debug, Clone)]
pub struct Regexp {
    pub op: Op,
    pub flags: u16,
    pub sub: Vec<Regexp>,
    pub rune: Vec<i32>,
    pub min: i32,
    pub max: i32,
    // Kept for parity with Go's Regexp; `name` is consulted when serializing
    // captures and holds the raw source text for `Op::UnicodeClass` nodes.
    #[allow(dead_code)]
    pub cap: i32,
    pub name: String,
}

impl Regexp {
    pub fn new(op: Op) -> Regexp {
        Regexp {
            op,
            flags: 0,
            sub: Vec::new(),
            rune: Vec::new(),
            min: 0,
            max: 0,
            cap: 0,
            name: String::new(),
        }
    }

    pub fn empty_match() -> Regexp {
        Regexp::new(Op::EmptyMatch)
    }

    // PORT NOTE: Go's `(*Regexp).Equal` is only needed during alternation
    // factoring; the parser ports it as `Parser::node_equal` on its arena,
    // so no owned-tree version exists here.
}

/// A parse error (Go `syntax.Error`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub code: ErrorCode,
    pub expr: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    // The full Go error-code set is ported for parity; not every code is
    // reachable from the supported syntax subset.
    #[allow(dead_code)]
    InvalidCharClass,
    InvalidCharRange,
    InvalidEscape,
    InvalidNamedCapture,
    InvalidPerlOp,
    InvalidRepeatOp,
    InvalidRepeatSize,
    MissingBracket,
    MissingParen,
    MissingRepeatArgument,
    TrailingBackslash,
    UnexpectedParen,
    NestingDepth,
    Large,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::InvalidCharClass => "invalid character class",
            ErrorCode::InvalidCharRange => "invalid character class range",
            ErrorCode::InvalidEscape => "invalid escape sequence",
            ErrorCode::InvalidNamedCapture => "invalid named capture",
            ErrorCode::InvalidPerlOp => "invalid or unsupported Perl syntax",
            ErrorCode::InvalidRepeatOp => "invalid nested repetition operator",
            ErrorCode::InvalidRepeatSize => "invalid repeat count",
            ErrorCode::MissingBracket => "missing closing ]",
            ErrorCode::MissingParen => "missing closing )",
            ErrorCode::MissingRepeatArgument => "missing argument to repetition operator",
            ErrorCode::TrailingBackslash => "trailing backslash at end of expression",
            ErrorCode::UnexpectedParen => "unexpected )",
            ErrorCode::NestingDepth => "expression nests too deeply",
            ErrorCode::Large => "expression too large",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "error parsing regexp: {}: `{}`",
            self.code.as_str(),
            self.expr
        )
    }
}

impl std::error::Error for Error {}

// Simplify.

/// Returns a regexp equivalent to `re` but without counted repetitions
/// and with various other simplifications (Go `(*Regexp).Simplify`).
///
/// PORT NOTE: Go shares subtrees in the result; this port clones them,
/// which changes nothing semantically.
pub fn simplify(re: &Regexp) -> Regexp {
    match re.op {
        Op::Capture | Op::Concat | Op::Alternate => {
            let mut nre = re.clone();
            nre.sub = re.sub.iter().map(simplify).collect();
            nre
        }
        Op::Star | Op::Plus | Op::Quest => {
            let sub = simplify(&re.sub[0]);
            simplify1(re.op, re.flags, sub)
        }
        Op::Repeat => {
            // Special special case: x{0} matches the empty string
            // and doesn't even need to consider x.
            if re.min == 0 && re.max == 0 {
                return Regexp::empty_match();
            }

            // The fun begins.
            let sub = simplify(&re.sub[0]);

            // x{n,} means at least n matches of x.
            if re.max == -1 {
                // Special case: x{0,} is x*.
                if re.min == 0 {
                    return simplify1(Op::Star, re.flags, sub);
                }
                // Special case: x{1,} is x+.
                if re.min == 1 {
                    return simplify1(Op::Plus, re.flags, sub);
                }
                // General case: x{4,} is xxxx+.
                let mut nre = Regexp::new(Op::Concat);
                for _ in 0..re.min - 1 {
                    nre.sub.push(sub.clone());
                }
                nre.sub.push(simplify1(Op::Plus, re.flags, sub));
                return nre;
            }

            // Special case x{0} handled above.

            // Special case: x{1} is just x.
            if re.min == 1 && re.max == 1 {
                return sub;
            }

            // General case: x{n,m} means n copies of x and m copies of x?
            // The machine will do less work if we nest the final m copies,
            // so that x{2,5} = xx(x(x(x)?)?)?

            // Build leading prefix: xx.
            let mut prefix: Option<Regexp> = None;
            if re.min > 0 {
                let mut p = Regexp::new(Op::Concat);
                for _ in 0..re.min {
                    p.sub.push(sub.clone());
                }
                prefix = Some(p);
            }

            // Build and attach suffix: (x(x(x)?)?)?
            if re.max > re.min {
                let mut suffix = simplify1(Op::Quest, re.flags, sub.clone());
                for _ in re.min + 1..re.max {
                    let mut nre2 = Regexp::new(Op::Concat);
                    nre2.sub.push(sub.clone());
                    nre2.sub.push(suffix);
                    suffix = simplify1(Op::Quest, re.flags, nre2);
                }
                match prefix {
                    None => return suffix,
                    Some(ref mut p) => p.sub.push(suffix),
                }
            }
            if let Some(p) = prefix {
                return p;
            }

            // Some degenerate case like min > max or min < max < 0.
            // Handle as impossible match.
            Regexp::new(Op::NoMatch)
        }
        _ => re.clone(),
    }
}

/// Go `simplify1`: Simplify for the unary OpStar, OpPlus and OpQuest.
fn simplify1(op: Op, flags: u16, sub: Regexp) -> Regexp {
    // Special case: repeat the empty string as much as
    // you want, but it's still the empty string.
    if sub.op == Op::EmptyMatch {
        return sub;
    }
    // The operators are idempotent if the flags match.
    if op == sub.op && flags & NON_GREEDY == sub.flags & NON_GREEDY {
        return sub;
    }
    let mut re = Regexp::new(op);
    re.flags = flags;
    re.sub.push(sub);
    re
}

// String serialization (Go 1.21+ semantics with flag hoisting).

// printFlags bits.
const FLAG_I: u8 = 1 << 0; // (?i:
const FLAG_M: u8 = 1 << 1; // (?m:
const FLAG_S: u8 = 1 << 2; // (?s:
const FLAG_OFF: u8 = 1 << 3; // )
const FLAG_PREC: u8 = 1 << 4; // (?: )
const NEG_SHIFT: u8 = 5; // FLAG_I<<NEG_SHIFT is (?-i:

type PrintFlagsMap = HashMap<usize, u8>;

fn key(re: &Regexp) -> usize {
    re as *const Regexp as usize
}

/// Enables the flags `f` around `start..last`.
fn add_span(start: &Regexp, last: &Regexp, f: u8, flags: &mut PrintFlagsMap) {
    flags.insert(key(start), f);
    *flags.entry(key(last)).or_insert(0) |= FLAG_OFF; // maybe start==last
}

/// Calculates the flags to print around each subexpression in `re`
/// (Go `calcFlags`). Returns `(must, cant)`.
fn calc_flags(re: &Regexp, flags: &mut PrintFlagsMap) -> (u8, u8) {
    match re.op {
        Op::Literal => {
            // If literal is fold-sensitive, return (flagI, 0) or (0, flagI)
            // according to whether (?i) is active.
            for &r in &re.rune {
                if (MIN_FOLD..=MAX_FOLD).contains(&r) && simple_fold(r) != r {
                    if re.flags & FOLD_CASE != 0 {
                        return (FLAG_I, 0);
                    }
                    return (0, FLAG_I);
                }
            }
            (0, 0)
        }
        Op::CharClass => {
            // If literal is fold-sensitive, return 0, flagI - (?i) has been compiled out.
            let mut i = 0;
            while i < re.rune.len() {
                let lo = re.rune[i].max(MIN_FOLD);
                let hi = re.rune[i + 1].min(MAX_FOLD);
                let mut r = lo;
                while r <= hi {
                    let mut f = simple_fold(r);
                    while f != r {
                        if !((lo..=hi).contains(&f) || in_char_class(f, &re.rune)) {
                            return (0, FLAG_I);
                        }
                        f = simple_fold(f);
                    }
                    r += 1;
                }
                i += 2;
            }
            (0, 0)
        }
        Op::UnicodeClass => {
            // Like a fold-sensitive literal: the class is opaque (its runes
            // are unknown), so it must keep — or must not gain — (?i).
            if re.flags & FOLD_CASE != 0 {
                (FLAG_I, 0)
            } else {
                (0, FLAG_I)
            }
        }
        Op::AnyCharNotNL => (0, FLAG_S),            // (?-s).
        Op::AnyChar => (FLAG_S, 0),                 // (?s).
        Op::BeginLine | Op::EndLine => (FLAG_M, 0), // (?m)^ (?m)$
        Op::EndText => {
            if re.flags & WAS_DOLLAR != 0 {
                // (?-m)$
                return (0, FLAG_M);
            }
            (0, 0)
        }
        Op::Capture | Op::Star | Op::Plus | Op::Quest | Op::Repeat => calc_flags(&re.sub[0], flags),
        Op::Concat | Op::Alternate => {
            // Gather the must and cant for each subexpression.
            // When we find a conflicting subexpression, insert the necessary
            // flags around the previously identified span and start over.
            let mut must: u8 = 0;
            let mut cant: u8 = 0;
            let mut all_cant: u8 = 0;
            let mut start = 0usize;
            let mut last = 0usize;
            let mut did = false;
            for (i, sub) in re.sub.iter().enumerate() {
                let (sub_must, sub_cant) = calc_flags(sub, flags);
                if must & sub_cant != 0 || sub_must & cant != 0 {
                    if must != 0 {
                        add_span(&re.sub[start], &re.sub[last], must, flags);
                    }
                    must = 0;
                    cant = 0;
                    start = i;
                    did = true;
                }
                must |= sub_must;
                cant |= sub_cant;
                all_cant |= sub_cant;
                if sub_must != 0 {
                    last = i;
                }
                if must == 0 && start == i {
                    start += 1;
                }
            }
            if !did {
                // No conflicts: pass the accumulated must and cant upward.
                return (must, cant);
            }
            if must != 0 {
                // Conflicts found; need to finish final span.
                add_span(&re.sub[start], &re.sub[last], must, flags);
            }
            (0, all_cant)
        }
        _ => (0, 0),
    }
}

/// Writes the Perl syntax for the regular expression `re` to `b`
/// (Go `writeRegexp`).
fn write_regexp(b: &mut String, re: &Regexp, f: u8, flags: &PrintFlagsMap) {
    let mut f = f | flags.get(&key(re)).copied().unwrap_or(0);
    if f & FLAG_PREC != 0 && f & !(FLAG_OFF | FLAG_PREC) != 0 && f & FLAG_OFF != 0 {
        // flagPrec is redundant with other flags being added and terminated
        f &= !FLAG_PREC;
    }
    if f & !(FLAG_OFF | FLAG_PREC) != 0 {
        b.push_str("(?");
        if f & FLAG_I != 0 {
            b.push('i');
        }
        if f & FLAG_M != 0 {
            b.push('m');
        }
        if f & FLAG_S != 0 {
            b.push('s');
        }
        if f & ((FLAG_M | FLAG_S) << NEG_SHIFT) != 0 {
            b.push('-');
            if f & (FLAG_M << NEG_SHIFT) != 0 {
                b.push('m');
            }
            if f & (FLAG_S << NEG_SHIFT) != 0 {
                b.push('s');
            }
        }
        b.push(':');
    }
    if f & FLAG_PREC != 0 {
        b.push_str("(?:");
    }

    match re.op {
        Op::NoMatch => b.push_str(r"[^\x00-\x{10FFFF}]"),
        Op::EmptyMatch => b.push_str("(?:)"),
        Op::Literal => {
            for &r in &re.rune {
                escape(b, r, false);
            }
        }
        Op::CharClass => {
            if !re.rune.len().is_multiple_of(2) {
                b.push_str("[invalid char class]");
            } else {
                b.push('[');
                if re.rune.is_empty() {
                    b.push_str(r"^\x00-\x{10FFFF}");
                } else if re.rune[0] == 0
                    && re.rune[re.rune.len() - 1] == MAX_RUNE
                    && re.rune.len() > 2
                {
                    // Contains 0 and MaxRune. Probably a negated class.
                    // Print the gaps.
                    b.push('^');
                    let mut i = 1;
                    while i < re.rune.len() - 1 {
                        let (lo, hi) = (re.rune[i] + 1, re.rune[i + 1] - 1);
                        escape(b, lo, lo == '-' as i32);
                        if lo != hi {
                            if hi != lo + 1 {
                                b.push('-');
                            }
                            escape(b, hi, hi == '-' as i32);
                        }
                        i += 2;
                    }
                } else {
                    let mut i = 0;
                    while i < re.rune.len() {
                        let (lo, hi) = (re.rune[i], re.rune[i + 1]);
                        escape(b, lo, lo == '-' as i32);
                        if lo != hi {
                            if hi != lo + 1 {
                                b.push('-');
                            }
                            escape(b, hi, hi == '-' as i32);
                        }
                        i += 2;
                    }
                }
                b.push(']');
            }
        }
        Op::UnicodeClass => b.push_str(&re.name),
        Op::AnyCharNotNL | Op::AnyChar => b.push('.'),
        Op::BeginLine => b.push('^'),
        Op::EndLine => b.push('$'),
        Op::BeginText => b.push_str(r"\A"),
        Op::EndText => {
            if re.flags & WAS_DOLLAR != 0 {
                b.push('$');
            } else {
                b.push_str(r"\z");
            }
        }
        Op::WordBoundary => b.push_str(r"\b"),
        Op::NoWordBoundary => b.push_str(r"\B"),
        Op::Capture => {
            if !re.name.is_empty() {
                b.push_str("(?P<");
                b.push_str(&re.name);
                b.push('>');
            } else {
                b.push('(');
            }
            if re.sub[0].op != Op::EmptyMatch {
                let sub_flags = flags.get(&key(&re.sub[0])).copied().unwrap_or(0);
                write_regexp(b, &re.sub[0], sub_flags, flags);
            }
            b.push(')');
        }
        Op::Star | Op::Plus | Op::Quest | Op::Repeat => {
            let sub = &re.sub[0];
            // An opaque Unicode class token binds like a char class, so it
            // needs no grouping parens under a repetition operator.
            let p = if (sub.op > Op::Capture && sub.op != Op::UnicodeClass)
                || (sub.op == Op::Literal && sub.rune.len() > 1)
            {
                FLAG_PREC
            } else {
                0
            };
            write_regexp(b, sub, p, flags);

            match re.op {
                Op::Star => b.push('*'),
                Op::Plus => b.push('+'),
                Op::Quest => b.push('?'),
                Op::Repeat => {
                    b.push('{');
                    write!(b, "{}", re.min).unwrap();
                    if re.max != re.min {
                        b.push(',');
                        if re.max >= 0 {
                            write!(b, "{}", re.max).unwrap();
                        }
                    }
                    b.push('}');
                }
                _ => unreachable!(),
            }
            if re.flags & NON_GREEDY != 0 {
                b.push('?');
            }
        }
        Op::Concat => {
            for sub in &re.sub {
                let p = if sub.op == Op::Alternate {
                    FLAG_PREC
                } else {
                    0
                };
                write_regexp(b, sub, p, flags);
            }
        }
        Op::Alternate => {
            for (i, sub) in re.sub.iter().enumerate() {
                if i > 0 {
                    b.push('|');
                }
                write_regexp(b, sub, 0, flags);
            }
        }
        _ => {
            write!(b, "<invalid op{}>", re.op as u8).unwrap();
        }
    }

    if f & FLAG_PREC != 0 {
        b.push(')');
    }
    if f & FLAG_OFF != 0 {
        b.push(')');
    }
}

impl fmt::Display for Regexp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut flags: PrintFlagsMap = HashMap::new();
        let (must, cant) = calc_flags(self, &mut flags);
        let mut must = must | ((cant & !FLAG_I) << NEG_SHIFT);
        if must != 0 {
            must |= FLAG_OFF;
        }
        let mut b = String::new();
        write_regexp(&mut b, self, must, &flags);
        f.write_str(&b)
    }
}

const META: &str = r"\.+*?()|[]{}^$";

fn escape(b: &mut String, r: i32, force: bool) {
    if is_print(r) {
        let c = char::from_u32(r as u32).unwrap();
        if META.contains(c) || force {
            b.push('\\');
        }
        b.push(c);
        return;
    }

    match r {
        0x07 => b.push_str(r"\a"),
        0x0c => b.push_str(r"\f"),
        0x0a => b.push_str(r"\n"),
        0x0d => b.push_str(r"\r"),
        0x09 => b.push_str(r"\t"),
        0x0b => b.push_str(r"\v"),
        _ => {
            if (0..0x100).contains(&r) {
                write!(b, "\\x{r:02x}").unwrap();
            } else {
                write!(b, "\\x{{{r:x}}}").unwrap();
            }
        }
    }
}

/// PORT NOTE: approximation of Go's `unicode.IsPrint` (categories L, M, N,
/// P, S plus ASCII space). It is exact for Latin-1; above that it treats any
/// non-control (Cc), non-whitespace code point as printable, because Rust
/// std exposes no general-category tables. Concretely: Cf format chars
/// (e.g. U+200B ZERO WIDTH SPACE) and unassigned code points serialize raw
/// where Go writes `\x{200b}`. This affects only the simplified-suffix TEXT
/// (an internal intermediate); both forms parse back to the same tree and
/// compile to the same matcher, so match results are unaffected.
fn is_print(r: i32) -> bool {
    if r < 0 {
        return false;
    }
    if r < 0x100 {
        if (0x20..=0x7e).contains(&r) {
            return true;
        }
        return (0xa1..=0xff).contains(&r) && r != 0xad;
    }
    match char::from_u32(r as u32) {
        None => false,
        Some(c) => !c.is_control() && !c.is_whitespace(),
    }
}

/// Reports whether `r` is in the class (which must be cleaned).
/// Go `inCharClass`.
pub fn in_char_class(r: i32, class: &[i32]) -> bool {
    let mut lo = 0usize;
    let mut hi = class.len() / 2;
    while lo < hi {
        let m = (lo + hi) / 2;
        if r > class[2 * m + 1] {
            lo = m + 1;
        } else if r < class[2 * m] {
            hi = m;
        } else {
            return true;
        }
    }
    false
}
