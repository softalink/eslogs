//! Port of Go's `regexp/syntax` parser (`parse.go`), including literal
//! concatenation, alternation factoring, char class canonicalization and the
//! height/size limits. The parser works on an index-based arena mirroring
//! Go's pointer-based `*Regexp` mutation, then exports an owned
//! [`Regexp`] tree.
//!
//! PORT NOTE: `\p{...}` / `\P{...}` Unicode character classes require the Go
//! `unicode` package tables, which are not ported; the tokens are kept
//! opaque (`Op::UnicodeClass`) and resolved by the regex crate when the
//! matcher is compiled. See `Parser::parse_unicode_class_token`.
//!
//! PORT NOTE: Go validates UTF-8 while parsing (`ErrInvalidUTF8`); Rust
//! strings are always valid UTF-8, so those paths do not exist here.

use std::collections::HashMap;

use super::goclass::{
    CharGroup, append_class, append_folded_class, append_folded_range, append_literal,
    append_range, clean_class, negate_class, perl_group, posix_group,
};
use super::gofold::{MAX_FOLD, MIN_FOLD, simple_fold};
use super::gosyntax::{
    CLASS_NL, DOT_NL, Error, ErrorCode, FOLD_CASE, LITERAL, MAX_RUNE, NON_GREEDY, ONE_LINE,
    OP_PSEUDO, Op, PERL_X, Regexp, UNICODE_GROUPS, WAS_DOLLAR,
};

type PResult<T> = Result<T, Error>;

/// Go `maxHeight`: the maximum height of a regexp parse tree.
const MAX_HEIGHT: i32 = 1000;

/// Go `maxSize`: the maximum size of a compiled regexp in Insts.
/// instSize = 5 * 8 (byte, 2 uint32, slice is 5 64-bit words).
const MAX_SIZE: i64 = (128 << 20) / 40;

/// Go `maxRunes`: the maximum number of runes allowed in a regexp tree.
const MAX_RUNES: usize = (128 << 20) / 4;

#[derive(Default)]
struct Node {
    op_raw: u8,
    flags: u16,
    sub: Vec<usize>,
    rune: Vec<i32>,
    min: i32,
    max: i32,
    cap: i32,
    name: String,
}

impl Node {
    fn op(&self) -> Op {
        // SAFETY-free decode: op_raw is only ever set from an Op value.
        match self.op_raw {
            1 => Op::NoMatch,
            2 => Op::EmptyMatch,
            3 => Op::Literal,
            4 => Op::CharClass,
            5 => Op::AnyCharNotNL,
            6 => Op::AnyChar,
            7 => Op::BeginLine,
            8 => Op::EndLine,
            9 => Op::BeginText,
            10 => Op::EndText,
            11 => Op::WordBoundary,
            12 => Op::NoWordBoundary,
            13 => Op::Capture,
            14 => Op::Star,
            15 => Op::Plus,
            16 => Op::Quest,
            17 => Op::Repeat,
            18 => Op::Concat,
            19 => Op::Alternate,
            20 => Op::UnicodeClass,
            128 => Op::PseudoLeftParen,
            _ => Op::PseudoVerticalBar,
        }
    }
}

struct Parser<'a> {
    flags: u16,
    stack: Vec<usize>,
    free: Vec<usize>,
    num_cap: i32,
    whole_regexp: &'a str,
    nodes: Vec<Node>,
    num_regexp: usize,
    num_runes: usize,
    repeats: i64,
    height: Option<HashMap<usize, i32>>,
    size: Option<HashMap<usize, i64>>,
}

fn next_rune(t: &str) -> (i32, &str) {
    let mut it = t.chars();
    match it.next() {
        Some(c) => (c as i32, it.as_str()),
        // Go's utf8.DecodeRuneInString("") returns RuneError with size 0,
        // which nextRune passes through without error.
        None => (0xFFFD, t),
    }
}

fn is_alnum(c: i32) -> bool {
    (48..=57).contains(&c) || (65..=90).contains(&c) || (97..=122).contains(&c)
}

fn unhex(c: i32) -> i32 {
    match c {
        0x30..=0x39 => c - 0x30,
        0x61..=0x66 => c - 0x61 + 10,
        0x41..=0x46 => c - 0x41 + 10,
        _ => -1,
    }
}

/// Go `minFoldRune`: returns the minimum rune fold-equivalent to `r`.
fn min_fold_rune(r: i32) -> i32 {
    if !(MIN_FOLD..=MAX_FOLD).contains(&r) {
        return r;
    }
    let mut m = r;
    let r0 = r;
    let mut r = simple_fold(r);
    while r != r0 {
        m = m.min(r);
        r = simple_fold(r);
    }
    m
}

/// Go `isValidCaptureName`: [A-Za-z0-9_]+.
fn is_valid_capture_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c == '_' || is_alnum(c as i32))
}

impl<'a> Parser<'a> {
    fn new(whole_regexp: &'a str, flags: u16) -> Parser<'a> {
        Parser {
            flags,
            stack: Vec::new(),
            free: Vec::new(),
            num_cap: 0,
            whole_regexp,
            nodes: Vec::new(),
            num_regexp: 0,
            num_runes: 0,
            repeats: 0,
            height: None,
            size: None,
        }
    }

    fn err(&self, code: ErrorCode, expr: &str) -> Error {
        Error {
            code,
            expr: expr.to_string(),
        }
    }

    fn op_of(&self, id: usize) -> Op {
        self.nodes[id].op()
    }

    fn new_regexp(&mut self, op: Op) -> usize {
        let node = Node {
            op_raw: op as u8,
            ..Node::default()
        };
        if let Some(id) = self.free.pop() {
            self.nodes[id] = node;
            id
        } else {
            self.nodes.push(node);
            self.num_regexp += 1;
            self.nodes.len() - 1
        }
    }

    fn reuse(&mut self, id: usize) {
        if let Some(h) = &mut self.height {
            h.remove(&id);
        }
        self.free.push(id);
    }

    fn check_limits(&mut self, id: usize) -> PResult<()> {
        if self.num_runes > MAX_RUNES {
            return Err(self.err(ErrorCode::Large, self.whole_regexp));
        }
        self.check_size(id)?;
        self.check_height(id)
    }

    fn check_size(&mut self, id: usize) -> PResult<()> {
        if self.size.is_none() {
            // We haven't started tracking size yet.
            // Do a relatively cheap check to see if we need to start.
            if self.repeats == 0 {
                self.repeats = 1;
            }
            if self.op_of(id) == Op::Repeat {
                let mut n = self.nodes[id].max;
                if n == -1 {
                    n = self.nodes[id].min;
                }
                if n <= 0 {
                    n = 1;
                }
                if n as i64 > MAX_SIZE / self.repeats {
                    self.repeats = MAX_SIZE;
                } else {
                    self.repeats *= n as i64;
                }
            }
            if (self.num_regexp as i64) < MAX_SIZE / self.repeats {
                return Ok(());
            }

            // We need to start tracking size.
            // Make the map and belatedly populate it
            // with info about everything we've constructed so far.
            self.size = Some(HashMap::new());
            let stack = self.stack.clone();
            for re in stack {
                self.check_size(re)?;
            }
        }

        if self.calc_size(id, true) > MAX_SIZE {
            return Err(self.err(ErrorCode::Large, self.whole_regexp));
        }
        Ok(())
    }

    fn calc_size(&mut self, id: usize, force: bool) -> i64 {
        if !force && let Some(&size) = self.size.as_ref().unwrap().get(&id) {
            return size;
        }

        let mut size: i64 = 0;
        match self.op_of(id) {
            Op::Literal => size = self.nodes[id].rune.len() as i64,
            Op::Capture | Op::Star => {
                // star can be 1+ or 2+; assume 2 pessimistically
                size = 2 + self.calc_size(self.nodes[id].sub[0], false);
            }
            Op::Plus | Op::Quest => {
                size = 1 + self.calc_size(self.nodes[id].sub[0], false);
            }
            Op::Concat => {
                for i in 0..self.nodes[id].sub.len() {
                    size += self.calc_size(self.nodes[id].sub[i], false);
                }
            }
            Op::Alternate => {
                for i in 0..self.nodes[id].sub.len() {
                    size += self.calc_size(self.nodes[id].sub[i], false);
                }
                let n = self.nodes[id].sub.len() as i64;
                if n > 1 {
                    size += n - 1;
                }
            }
            Op::Repeat => {
                let sub = self.calc_size(self.nodes[id].sub[0], false);
                let (min, max) = (self.nodes[id].min as i64, self.nodes[id].max as i64);
                if max == -1 {
                    if min == 0 {
                        size = 2 + sub; // x*
                    } else {
                        size = 1 + min * sub; // xxx+
                    }
                } else {
                    // x{2,5} = xx(x(x(x)?)?)?
                    size = max * sub + (max - min);
                }
            }
            _ => {}
        }

        size = size.max(1);
        self.size.as_mut().unwrap().insert(id, size);
        size
    }

    fn check_height(&mut self, id: usize) -> PResult<()> {
        if self.num_regexp < MAX_HEIGHT as usize {
            return Ok(());
        }
        if self.height.is_none() {
            self.height = Some(HashMap::new());
            let stack = self.stack.clone();
            for re in stack {
                self.check_height(re)?;
            }
        }
        if self.calc_height(id, true) > MAX_HEIGHT {
            return Err(self.err(ErrorCode::NestingDepth, self.whole_regexp));
        }
        Ok(())
    }

    fn calc_height(&mut self, id: usize, force: bool) -> i32 {
        if !force && let Some(&h) = self.height.as_ref().unwrap().get(&id) {
            return h;
        }
        let mut h = 1;
        for i in 0..self.nodes[id].sub.len() {
            let hsub = self.calc_height(self.nodes[id].sub[i], false);
            if h < 1 + hsub {
                h = 1 + hsub;
            }
        }
        self.height.as_mut().unwrap().insert(id, h);
        h
    }

    // Parse stack manipulation.

    /// Pushes the regexp `id` onto the parse stack and returns it.
    /// Returns `None` if the rune was merged into an existing literal.
    fn push(&mut self, id: usize) -> PResult<Option<usize>> {
        self.num_runes += self.nodes[id].rune.len();
        let op = self.op_of(id);
        let rune = &self.nodes[id].rune;
        let single = op == Op::CharClass && rune.len() == 2 && rune[0] == rune[1];
        let fold_pair = op == Op::CharClass
            && ((rune.len() == 4
                && rune[0] == rune[1]
                && rune[2] == rune[3]
                && simple_fold(rune[0]) == rune[2]
                && simple_fold(rune[2]) == rune[0])
                || (rune.len() == 2
                    && rune[0] + 1 == rune[1]
                    && simple_fold(rune[0]) == rune[1]
                    && simple_fold(rune[1]) == rune[0]));
        if single {
            // Single rune.
            let r0 = self.nodes[id].rune[0];
            if self.maybe_concat(r0, self.flags & !FOLD_CASE) {
                return Ok(None);
            }
            let n = &mut self.nodes[id];
            n.op_raw = Op::Literal as u8;
            n.rune.truncate(1);
            n.flags = self.flags & !FOLD_CASE;
        } else if fold_pair {
            // Case-insensitive rune like [Aa] or [Δδ].
            let r0 = self.nodes[id].rune[0];
            if self.maybe_concat(r0, self.flags | FOLD_CASE) {
                return Ok(None);
            }
            // Rewrite as (case-insensitive) literal.
            let n = &mut self.nodes[id];
            n.op_raw = Op::Literal as u8;
            n.rune.truncate(1);
            n.flags = self.flags | FOLD_CASE;
        } else {
            // Incremental concatenation.
            self.maybe_concat(-1, 0);
        }

        self.stack.push(id);
        self.check_limits(id)?;
        Ok(Some(id))
    }

    /// Go `maybeConcat`: incremental concatenation of literal runes into
    /// string nodes. Reports whether `r` was pushed.
    fn maybe_concat(&mut self, r: i32, flags: u16) -> bool {
        let n = self.stack.len();
        if n < 2 {
            return false;
        }

        let re1 = self.stack[n - 1];
        let re2 = self.stack[n - 2];
        if self.op_of(re1) != Op::Literal
            || self.op_of(re2) != Op::Literal
            || self.nodes[re1].flags & FOLD_CASE != self.nodes[re2].flags & FOLD_CASE
        {
            return false;
        }

        // Push re1 into re2.
        let mut runes = std::mem::take(&mut self.nodes[re1].rune);
        self.nodes[re2].rune.extend_from_slice(&runes);

        // Reuse re1 if possible.
        if r >= 0 {
            runes.clear();
            runes.push(r);
            self.nodes[re1].rune = runes;
            self.nodes[re1].flags = flags;
            return true;
        }

        self.stack.pop();
        self.reuse(re1);
        false // did not push r
    }

    /// Go `literal`: pushes a literal regexp for the rune `r` on the stack.
    fn literal(&mut self, mut r: i32) -> PResult<()> {
        let re = self.new_regexp(Op::Literal);
        self.nodes[re].flags = self.flags;
        if self.flags & FOLD_CASE != 0 {
            r = min_fold_rune(r);
        }
        self.nodes[re].rune.push(r);
        self.push(re)?;
        Ok(())
    }

    /// Go `op`: pushes a regexp with the given op onto the stack.
    fn op(&mut self, op: Op) -> PResult<usize> {
        let re = self.new_regexp(op);
        self.nodes[re].flags = self.flags;
        self.push(re)?;
        Ok(re)
    }

    /// Go `repeat`: replaces the top stack element with itself repeated.
    fn repeat(
        &mut self,
        op: Op,
        min: i32,
        max: i32,
        before: &'a str,
        after: &'a str,
        last_repeat: &'a str,
    ) -> PResult<&'a str> {
        let mut flags = self.flags;
        let mut after = after;
        if self.flags & PERL_X != 0 {
            if !after.is_empty() && after.as_bytes()[0] == b'?' {
                after = &after[1..];
                flags ^= NON_GREEDY;
            }
            if !last_repeat.is_empty() {
                // In Perl it is not allowed to stack repetition operators:
                // a** is a syntax error, not a doubled star, and a++ means
                // something else entirely, which we don't support!
                return Err(self.err(
                    ErrorCode::InvalidRepeatOp,
                    &last_repeat[..last_repeat.len() - after.len()],
                ));
            }
        }
        let n = self.stack.len();
        if n == 0 {
            return Err(self.err(
                ErrorCode::MissingRepeatArgument,
                &before[..before.len() - after.len()],
            ));
        }
        let sub = self.stack[n - 1];
        if self.op_of(sub) as u8 >= OP_PSEUDO {
            return Err(self.err(
                ErrorCode::MissingRepeatArgument,
                &before[..before.len() - after.len()],
            ));
        }

        let re = self.new_regexp(op);
        {
            let node = &mut self.nodes[re];
            node.min = min;
            node.max = max;
            node.flags = flags;
            node.sub = vec![sub];
        }
        self.stack[n - 1] = re;
        self.check_limits(re)?;

        if op == Op::Repeat && (min >= 2 || max >= 2) && !self.repeat_is_valid(re, 1000) {
            return Err(self.err(
                ErrorCode::InvalidRepeatSize,
                &before[..before.len() - after.len()],
            ));
        }

        Ok(after)
    }

    /// Go `repeatIsValid`.
    fn repeat_is_valid(&self, id: usize, n: i32) -> bool {
        let mut n = n;
        if self.op_of(id) == Op::Repeat {
            let mut m = self.nodes[id].max;
            if m == 0 {
                return true;
            }
            if m < 0 {
                m = self.nodes[id].min;
            }
            if m > n {
                return false;
            }
            if m > 0 {
                n /= m;
            }
        }
        for &sub in &self.nodes[id].sub {
            if !self.repeat_is_valid(sub, n) {
                return false;
            }
        }
        true
    }

    /// Go `concat`: replaces the top of the stack (above the topmost '|' or
    /// '(') with its concatenation.
    fn concat(&mut self) -> PResult<()> {
        self.maybe_concat(-1, 0);

        // Scan down to find pseudo-operator | or (.
        let mut i = self.stack.len();
        while i > 0 && (self.op_of(self.stack[i - 1]) as u8) < OP_PSEUDO {
            i -= 1;
        }
        let subs = self.stack.split_off(i);

        // Empty concatenation is special case.
        if subs.is_empty() {
            let re = self.new_regexp(Op::EmptyMatch);
            self.push(re)?;
            return Ok(());
        }

        let c = self.collapse(subs, Op::Concat)?;
        self.push(c)?;
        Ok(())
    }

    /// Go `alternate`: replaces the top of the stack (above the topmost '(')
    /// with its alternation.
    fn alternate(&mut self) -> PResult<()> {
        // Scan down to find pseudo-operator (.
        // There are no | above (.
        let mut i = self.stack.len();
        while i > 0 && (self.op_of(self.stack[i - 1]) as u8) < OP_PSEUDO {
            i -= 1;
        }
        let subs = self.stack.split_off(i);

        // Make sure top class is clean.
        // All the others already are (see swapVerticalBar).
        if let Some(&last) = subs.last() {
            self.clean_alt(last);
        }

        // Empty alternate is special case
        // (shouldn't happen but easy to handle).
        if subs.is_empty() {
            let re = self.new_regexp(Op::NoMatch);
            self.push(re)?;
            return Ok(());
        }

        let a = self.collapse(subs, Op::Alternate)?;
        self.push(a)?;
        Ok(())
    }

    /// Go `cleanAlt`: cleans `re` for eventual inclusion in an alternation.
    fn clean_alt(&mut self, id: usize) {
        if self.op_of(id) != Op::CharClass {
            return;
        }
        let mut rune = std::mem::take(&mut self.nodes[id].rune);
        clean_class(&mut rune);
        if rune.len() == 2 && rune[0] == 0 && rune[1] == MAX_RUNE {
            self.nodes[id].op_raw = Op::AnyChar as u8;
            return;
        }
        if rune.len() == 4
            && rune[0] == 0
            && rune[1] == '\n' as i32 - 1
            && rune[2] == '\n' as i32 + 1
            && rune[3] == MAX_RUNE
        {
            self.nodes[id].op_raw = Op::AnyCharNotNL as u8;
            return;
        }
        self.nodes[id].rune = rune;
    }

    /// Go `collapse`: returns the result of applying `op` to `subs`,
    /// hoisting nested nodes of the same op.
    fn collapse(&mut self, subs: Vec<usize>, op: Op) -> PResult<usize> {
        if subs.len() == 1 {
            return Ok(subs[0]);
        }
        let re = self.new_regexp(op);
        let mut new_sub: Vec<usize> = Vec::new();
        for sub in subs {
            if self.op_of(sub) == op {
                let inner = std::mem::take(&mut self.nodes[sub].sub);
                new_sub.extend(inner);
                self.reuse(sub);
            } else {
                new_sub.push(sub);
            }
        }
        self.nodes[re].sub = new_sub;
        if op == Op::Alternate {
            let subs = std::mem::take(&mut self.nodes[re].sub);
            let factored = self.factor(subs)?;
            self.nodes[re].sub = factored;
            if self.nodes[re].sub.len() == 1 {
                let inner = self.nodes[re].sub[0];
                self.reuse(re);
                return Ok(inner);
            }
        }
        Ok(re)
    }

    /// Go `factor`: factors common prefixes from the alternation list `sub`.
    // The index-based loops mirror the Go original, which mutates sub[j] in
    // place while iterating.
    #[allow(clippy::needless_range_loop)]
    fn factor(&mut self, mut sub: Vec<usize>) -> PResult<Vec<usize>> {
        if sub.len() < 2 {
            return Ok(sub);
        }

        // Round 1: Factor out common literal prefixes.
        let mut str_: Vec<i32> = Vec::new();
        let mut strflags: u16 = 0;
        let mut start = 0usize;
        let mut out: Vec<usize> = Vec::new();
        for i in 0..=sub.len() {
            // Invariant: sub[start:i] consists of regexps that all begin
            // with str as modified by strflags.
            let mut istr: Vec<i32> = Vec::new();
            let mut iflags: u16 = 0;
            if i < sub.len() {
                let (s, f) = self.leading_string(sub[i]);
                istr = s;
                iflags = f;
                if iflags == strflags {
                    let mut same = 0usize;
                    while same < str_.len() && same < istr.len() && str_[same] == istr[same] {
                        same += 1;
                    }
                    if same > 0 {
                        // Matches at least one rune in current range.
                        // Keep going around.
                        str_.truncate(same);
                        continue;
                    }
                }
            }

            // Found end of a run with common leading literal string:
            // sub[start:i] all begin with str[:len(str)], but sub[i]
            // does not even begin with str[0].
            if i == start {
                // Nothing to do - run of length 0.
            } else if i == start + 1 {
                // Just one: don't bother factoring.
                out.push(sub[start]);
            } else {
                // Construct factored form: prefix(suffix1|suffix2|...)
                let prefix = self.new_regexp(Op::Literal);
                self.nodes[prefix].flags = strflags;
                self.nodes[prefix].rune = str_.clone();

                for j in start..i {
                    sub[j] = self.remove_leading_string(sub[j], str_.len());
                    self.check_limits(sub[j])?;
                }
                let suffix = self.collapse(sub[start..i].to_vec(), Op::Alternate)?; // recurse

                let re = self.new_regexp(Op::Concat);
                self.nodes[re].sub = vec![prefix, suffix];
                out.push(re);
            }

            // Prepare for next iteration.
            start = i;
            str_ = istr;
            strflags = iflags;
        }
        sub = out;

        // Round 2: Factor out common simple prefixes,
        // just the first piece of each concatenation.
        start = 0;
        out = Vec::new();
        let mut first: Option<usize> = None;
        for i in 0..=sub.len() {
            // Invariant: sub[start:i] consists of regexps that all begin with first.
            let mut ifirst: Option<usize> = None;
            if i < sub.len() {
                ifirst = self.leading_regexp(sub[i]);
                if let (Some(f), Some(inf)) = (first, ifirst) {
                    // first must be a character class OR a fixed repeat of
                    // a character class.
                    let f_op = self.op_of(f);
                    if self.node_equal(f, inf)
                        && (self.is_char_class(f)
                            || (f_op == Op::Repeat
                                && self.nodes[f].min == self.nodes[f].max
                                && self.is_char_class(self.nodes[f].sub[0])))
                    {
                        continue;
                    }
                }
            }

            // Found end of a run with common leading regexp:
            // sub[start:i] all begin with first but sub[i] does not.
            if i == start {
                // Nothing to do - run of length 0.
            } else if i == start + 1 {
                // Just one: don't bother factoring.
                out.push(sub[start]);
            } else {
                // Construct factored form: prefix(suffix1|suffix2|...)
                let prefix = first.expect("BUG: run of factored regexps without leading regexp");
                for j in start..i {
                    let reuse = j != start; // prefix came from sub[start]
                    sub[j] = self.remove_leading_regexp(sub[j], reuse);
                    self.check_limits(sub[j])?;
                }
                let suffix = self.collapse(sub[start..i].to_vec(), Op::Alternate)?; // recurse

                let re = self.new_regexp(Op::Concat);
                self.nodes[re].sub = vec![prefix, suffix];
                out.push(re);
            }

            // Prepare for next iteration.
            start = i;
            first = ifirst;
        }
        sub = out;

        // Round 3: Collapse runs of single literals into character classes.
        start = 0;
        out = Vec::new();
        for i in 0..=sub.len() {
            // Invariant: sub[start:i] consists of regexps that are either
            // literal runes or character classes.
            if i < sub.len() && self.is_char_class(sub[i]) {
                continue;
            }

            // sub[i] is not a char or char class;
            // emit char class for sub[start:i]...
            if i == start {
                // Nothing to do - run of length 0.
            } else if i == start + 1 {
                out.push(sub[start]);
            } else {
                // Make new char class.
                // Start with most complex regexp in sub[start].
                let mut max = start;
                for j in start + 1..i {
                    let (mop, jop) = (self.op_of(sub[max]), self.op_of(sub[j]));
                    if mop < jop
                        || (mop == jop
                            && self.nodes[sub[max]].rune.len() < self.nodes[sub[j]].rune.len())
                    {
                        max = j;
                    }
                }
                sub.swap(start, max);

                for j in start + 1..i {
                    self.merge_char_class(sub[start], sub[j]);
                    self.reuse(sub[j]);
                }
                self.clean_alt(sub[start]);
                out.push(sub[start]);
            }

            // ... and then emit sub[i].
            if i < sub.len() {
                out.push(sub[i]);
            }
            start = i + 1;
        }
        sub = out;

        // Round 4: Collapse runs of empty matches into a single empty match.
        let mut out: Vec<usize> = Vec::new();
        for i in 0..sub.len() {
            if i + 1 < sub.len()
                && self.op_of(sub[i]) == Op::EmptyMatch
                && self.op_of(sub[i + 1]) == Op::EmptyMatch
            {
                continue;
            }
            out.push(sub[i]);
        }

        Ok(out)
    }

    /// Go `leadingString`: the leading literal string that `re` begins with.
    fn leading_string(&self, mut id: usize) -> (Vec<i32>, u16) {
        if self.op_of(id) == Op::Concat && !self.nodes[id].sub.is_empty() {
            id = self.nodes[id].sub[0];
        }
        if self.op_of(id) != Op::Literal {
            return (Vec::new(), 0);
        }
        (
            self.nodes[id].rune.clone(),
            self.nodes[id].flags & FOLD_CASE,
        )
    }

    /// Go `removeLeadingString`: removes the first `n` leading runes from the
    /// beginning of `re`.
    fn remove_leading_string(&mut self, id: usize, n: usize) -> usize {
        if self.op_of(id) == Op::Concat && !self.nodes[id].sub.is_empty() {
            // Removing a leading string in a concatenation
            // might simplify the concatenation.
            let sub0 = self.remove_leading_string(self.nodes[id].sub[0], n);
            self.nodes[id].sub[0] = sub0;
            if self.op_of(sub0) == Op::EmptyMatch {
                self.reuse(sub0);
                match self.nodes[id].sub.len() {
                    0 | 1 => {
                        // Impossible but handle.
                        self.nodes[id].op_raw = Op::EmptyMatch as u8;
                        self.nodes[id].sub.clear();
                    }
                    2 => {
                        let keep = self.nodes[id].sub[1];
                        self.reuse(id);
                        return keep;
                    }
                    _ => {
                        self.nodes[id].sub.remove(0);
                    }
                }
            }
            return id;
        }

        if self.op_of(id) == Op::Literal {
            self.nodes[id].rune.drain(..n);
            if self.nodes[id].rune.is_empty() {
                self.nodes[id].op_raw = Op::EmptyMatch as u8;
            }
        }
        id
    }

    /// Go `leadingRegexp`: the leading regexp that `re` begins with.
    fn leading_regexp(&self, id: usize) -> Option<usize> {
        if self.op_of(id) == Op::EmptyMatch {
            return None;
        }
        if self.op_of(id) == Op::Concat && !self.nodes[id].sub.is_empty() {
            let sub0 = self.nodes[id].sub[0];
            if self.op_of(sub0) == Op::EmptyMatch {
                return None;
            }
            return Some(sub0);
        }
        Some(id)
    }

    /// Go `removeLeadingRegexp`: removes the leading regexp in `re`.
    fn remove_leading_regexp(&mut self, id: usize, reuse: bool) -> usize {
        if self.op_of(id) == Op::Concat && !self.nodes[id].sub.is_empty() {
            if reuse {
                let sub0 = self.nodes[id].sub[0];
                self.reuse(sub0);
            }
            self.nodes[id].sub.remove(0);
            match self.nodes[id].sub.len() {
                0 => {
                    self.nodes[id].op_raw = Op::EmptyMatch as u8;
                    self.nodes[id].sub.clear();
                }
                1 => {
                    let keep = self.nodes[id].sub[0];
                    self.reuse(id);
                    return keep;
                }
                _ => {}
            }
            return id;
        }
        if reuse {
            self.reuse(id);
        }
        self.new_regexp(Op::EmptyMatch)
    }

    /// Arena version of Go `(*Regexp).Equal`.
    fn node_equal(&self, x: usize, y: usize) -> bool {
        let (nx, ny) = (&self.nodes[x], &self.nodes[y]);
        if nx.op() != ny.op() {
            return false;
        }
        match nx.op() {
            Op::EndText => {
                if nx.flags & WAS_DOLLAR != ny.flags & WAS_DOLLAR {
                    return false;
                }
            }
            Op::Literal | Op::CharClass => {
                return nx.flags & FOLD_CASE == ny.flags & FOLD_CASE && nx.rune == ny.rune;
            }
            Op::UnicodeClass => {
                // Opaque token: equal only for the same source text under
                // the same fold flag.
                return nx.flags & FOLD_CASE == ny.flags & FOLD_CASE && nx.name == ny.name;
            }
            Op::Alternate | Op::Concat => {
                return nx.sub.len() == ny.sub.len()
                    && nx
                        .sub
                        .iter()
                        .zip(ny.sub.iter())
                        .all(|(&a, &b)| self.node_equal(a, b));
            }
            Op::Star | Op::Plus | Op::Quest => {
                if nx.flags & NON_GREEDY != ny.flags & NON_GREEDY
                    || !self.node_equal(nx.sub[0], ny.sub[0])
                {
                    return false;
                }
            }
            Op::Repeat => {
                if nx.flags & NON_GREEDY != ny.flags & NON_GREEDY
                    || nx.min != ny.min
                    || nx.max != ny.max
                    || !self.node_equal(nx.sub[0], ny.sub[0])
                {
                    return false;
                }
            }
            Op::Capture => {
                return nx.cap == ny.cap
                    && nx.name == ny.name
                    && self.node_equal(nx.sub[0], ny.sub[0]);
            }
            _ => {}
        }
        true
    }

    /// Go `isCharClass`: can this be represented as a character class?
    fn is_char_class(&self, id: usize) -> bool {
        let n = &self.nodes[id];
        (n.op() == Op::Literal && n.rune.len() == 1)
            || n.op() == Op::CharClass
            || n.op() == Op::AnyCharNotNL
            || n.op() == Op::AnyChar
    }

    /// Go `matchRune`: does `re` match `r`?
    fn match_rune(&self, id: usize, r: i32) -> bool {
        let n = &self.nodes[id];
        match n.op() {
            Op::Literal => n.rune.len() == 1 && n.rune[0] == r,
            Op::CharClass => {
                let mut i = 0;
                while i < n.rune.len() {
                    if n.rune[i] <= r && r <= n.rune[i + 1] {
                        return true;
                    }
                    i += 2;
                }
                false
            }
            Op::AnyCharNotNL => r != '\n' as i32,
            Op::AnyChar => true,
            _ => false,
        }
    }

    /// Go `parseVerticalBar`: handles a | in the input.
    fn parse_vertical_bar(&mut self) -> PResult<()> {
        self.concat()?;

        // The concatenation we just parsed is on top of the stack.
        // If it sits above an opVerticalBar, swap it below
        // (things below an opVerticalBar become an alternation).
        // Otherwise, push a new vertical bar.
        if !self.swap_vertical_bar() {
            self.op(Op::PseudoVerticalBar)?;
        }
        Ok(())
    }

    /// Go `mergeCharClass`: makes dst = dst|src.
    /// The caller must ensure that dst.Op >= src.Op.
    fn merge_char_class(&mut self, dst: usize, src: usize) {
        match self.op_of(dst) {
            Op::AnyChar => {
                // src doesn't add anything.
            }
            Op::AnyCharNotNL => {
                // src might add \n
                if self.match_rune(src, '\n' as i32) {
                    self.nodes[dst].op_raw = Op::AnyChar as u8;
                }
            }
            Op::CharClass => {
                // src is simpler, so either literal or char class
                if self.op_of(src) == Op::Literal {
                    let (x, fold) = (
                        self.nodes[src].rune[0],
                        self.nodes[src].flags & FOLD_CASE != 0,
                    );
                    let mut rune = std::mem::take(&mut self.nodes[dst].rune);
                    append_literal(&mut rune, x, fold);
                    self.nodes[dst].rune = rune;
                } else {
                    let src_rune = self.nodes[src].rune.clone();
                    let mut rune = std::mem::take(&mut self.nodes[dst].rune);
                    append_class(&mut rune, &src_rune);
                    self.nodes[dst].rune = rune;
                }
            }
            Op::Literal => {
                // both literal
                if self.nodes[src].rune[0] == self.nodes[dst].rune[0]
                    && self.nodes[src].flags == self.nodes[dst].flags
                {
                    return;
                }
                let (d0, dflags) = (self.nodes[dst].rune[0], self.nodes[dst].flags);
                let (s0, sflags) = (self.nodes[src].rune[0], self.nodes[src].flags);
                let mut rune: Vec<i32> = Vec::new();
                append_literal(&mut rune, d0, dflags & FOLD_CASE != 0);
                append_literal(&mut rune, s0, sflags & FOLD_CASE != 0);
                let n = &mut self.nodes[dst];
                n.op_raw = Op::CharClass as u8;
                n.rune = rune;
            }
            _ => {}
        }
    }

    /// Go `swapVerticalBar`.
    fn swap_vertical_bar(&mut self) -> bool {
        // If above and below vertical bar are literal or char class,
        // can merge into a single char class.
        let n = self.stack.len();
        if n >= 3
            && self.op_of(self.stack[n - 2]) == Op::PseudoVerticalBar
            && self.is_char_class(self.stack[n - 1])
            && self.is_char_class(self.stack[n - 3])
        {
            let mut re1 = self.stack[n - 1];
            let mut re3 = self.stack[n - 3];
            // Make re3 the more complex of the two.
            if self.op_of(re1) > self.op_of(re3) {
                std::mem::swap(&mut re1, &mut re3);
                self.stack[n - 3] = re3;
            }
            self.merge_char_class(re3, re1);
            self.reuse(re1);
            self.stack.pop();
            return true;
        }

        if n >= 2 {
            let re1 = self.stack[n - 1];
            let re2 = self.stack[n - 2];
            if self.op_of(re2) == Op::PseudoVerticalBar {
                if n >= 3 {
                    // Now out of reach.
                    // Clean opportunistically.
                    let re3 = self.stack[n - 3];
                    self.clean_alt(re3);
                }
                self.stack[n - 2] = re1;
                self.stack[n - 1] = re2;
                return true;
            }
        }
        false
    }

    /// Go `parseRightParen`: handles a ) in the input.
    fn parse_right_paren(&mut self) -> PResult<()> {
        self.concat()?;
        if self.swap_vertical_bar() {
            // pop vertical bar
            self.stack.pop();
        }
        self.alternate()?;

        let n = self.stack.len();
        if n < 2 {
            return Err(self.err(ErrorCode::UnexpectedParen, self.whole_regexp));
        }
        let re1 = self.stack[n - 1];
        let re2 = self.stack[n - 2];
        self.stack.truncate(n - 2);
        if self.op_of(re2) != Op::PseudoLeftParen {
            return Err(self.err(ErrorCode::UnexpectedParen, self.whole_regexp));
        }
        // Restore flags at time of paren.
        self.flags = self.nodes[re2].flags;
        if self.nodes[re2].cap == 0 {
            // Just for grouping.
            self.push(re1)?;
        } else {
            self.nodes[re2].op_raw = Op::Capture as u8;
            self.nodes[re2].sub = vec![re1];
            self.push(re2)?;
        }
        Ok(())
    }

    /// Go `parsePerlFlags`: parses a Perl flag setting or non-capturing
    /// group or both, like (?i) or (?: or (?i:.
    fn parse_perl_flags(&mut self, s: &'a str) -> PResult<&'a str> {
        let t = s;
        let tb = t.as_bytes();

        // Check for named captures.
        let starts_with_p = t.len() > 4 && tb[2] == b'P' && tb[3] == b'<';
        let starts_with_name = t.len() > 3 && tb[2] == b'<';

        if starts_with_p || starts_with_name {
            // position of expr start
            let expr_start_pos = if starts_with_p { 4 } else { 3 };

            // Pull out name.
            let Some(end) = t.find('>') else {
                return Err(self.err(ErrorCode::InvalidNamedCapture, s));
            };

            let capture = &t[..end + 1]; // "(?P<name>" or "(?<name>"
            let name = &t[expr_start_pos..end]; // "name"
            if !is_valid_capture_name(name) {
                return Err(self.err(ErrorCode::InvalidNamedCapture, capture));
            }

            // Like ordinary capture, but named.
            self.num_cap += 1;
            let re = self.op(Op::PseudoLeftParen)?;
            self.nodes[re].cap = self.num_cap;
            self.nodes[re].name = name.to_string();
            return Ok(&t[end + 1..]);
        }

        // Non-capturing group. Might also twiddle Perl flags.
        let mut t = &t[2..]; // skip (?
        let mut flags = self.flags;
        let mut sign = 1i32;
        let mut saw_flag = false;
        'flag_loop: while !t.is_empty() {
            let (c, rest) = next_rune(t);
            t = rest;
            match c {
                // Flags.
                0x69 /* i */ => {
                    flags |= FOLD_CASE;
                    saw_flag = true;
                }
                0x6d /* m */ => {
                    flags &= !ONE_LINE;
                    saw_flag = true;
                }
                0x73 /* s */ => {
                    flags |= DOT_NL;
                    saw_flag = true;
                }
                0x55 /* U */ => {
                    flags |= NON_GREEDY;
                    saw_flag = true;
                }

                // Switch to negation.
                0x2d /* - */ => {
                    if sign < 0 {
                        break 'flag_loop;
                    }
                    sign = -1;
                    // Invert flags so that | above turn into &^ and vice versa.
                    // We'll invert flags again before using it below.
                    flags = !flags;
                    saw_flag = false;
                }

                // End of flags, starting group or not.
                0x3a /* : */ | 0x29 /* ) */ => {
                    if sign < 0 {
                        if !saw_flag {
                            break 'flag_loop;
                        }
                        flags = !flags;
                    }
                    if c == 0x3a {
                        // Open new group
                        self.op(Op::PseudoLeftParen)?;
                    }
                    self.flags = flags;
                    return Ok(t);
                }

                _ => break 'flag_loop,
            }
        }

        Err(self.err(ErrorCode::InvalidPerlOp, &s[..s.len() - t.len()]))
    }

    /// Go `parseInt`: parses a decimal integer.
    fn parse_int(&self, s: &'a str) -> (i32, &'a str, bool) {
        if s.is_empty() || !s.as_bytes()[0].is_ascii_digit() {
            return (0, s, false);
        }
        // Disallow leading zeros.
        if s.len() >= 2 && s.as_bytes()[0] == b'0' && s.as_bytes()[1].is_ascii_digit() {
            return (0, s, false);
        }
        let t = s;
        let mut s = s;
        while !s.is_empty() && s.as_bytes()[0].is_ascii_digit() {
            s = &s[1..];
        }
        let rest = s;
        // Have digits, compute value.
        let digits = &t[..t.len() - s.len()];
        let mut n: i32 = 0;
        for &b in digits.as_bytes() {
            // Avoid overflow.
            if n >= 100_000_000 {
                n = -1;
                break;
            }
            n = n * 10 + (b - b'0') as i32;
        }
        (n, rest, true)
    }

    /// Go `parseRepeat`: parses {min} (max=min) or {min,} (max=-1) or
    /// {min,max}. Returns `None` when `s` is not of that form.
    fn parse_repeat(&self, s: &'a str) -> Option<(i32, i32, &'a str)> {
        if s.is_empty() || s.as_bytes()[0] != b'{' {
            return None;
        }
        let mut s = &s[1..];
        let (mut min, rest, ok1) = self.parse_int(s);
        if !ok1 {
            return None;
        }
        s = rest;
        if s.is_empty() {
            return None;
        }
        let max;
        if s.as_bytes()[0] != b',' {
            max = min;
        } else {
            s = &s[1..];
            if s.is_empty() {
                return None;
            }
            if s.as_bytes()[0] == b'}' {
                max = -1;
            } else {
                let (m, rest, ok) = self.parse_int(s);
                if !ok {
                    return None;
                }
                max = m;
                s = rest;
                if max < 0 {
                    // parseInt found too big a number
                    min = -1;
                }
            }
        }
        if s.is_empty() || s.as_bytes()[0] != b'}' {
            return None;
        }
        Some((min, max, &s[1..]))
    }

    /// Go `parseEscape`: parses an escape sequence at the beginning of `s`.
    fn parse_escape(&self, s: &'a str) -> PResult<(i32, &'a str)> {
        let t0 = &s[1..];
        if t0.is_empty() {
            return Err(self.err(ErrorCode::TrailingBackslash, ""));
        }
        let (c, mut t) = next_rune(t0);

        'switch: {
            if c < 0x80 && !is_alnum(c) {
                // Escaped non-word characters are always themselves.
                return Ok((c, t));
            }

            match c {
                // Octal escapes.
                0x31..=0x37 /* '1'-'7' */ => {
                    // Single non-zero digit is a backreference; not supported
                    if t.is_empty() || t.as_bytes()[0] < b'0' || t.as_bytes()[0] > b'7' {
                        break 'switch;
                    }
                    // Consume up to three octal digits; already have one.
                    let mut r = c - 0x30;
                    for _ in 1..3 {
                        if t.is_empty() || t.as_bytes()[0] < b'0' || t.as_bytes()[0] > b'7' {
                            break;
                        }
                        r = r * 8 + (t.as_bytes()[0] - b'0') as i32;
                        t = &t[1..];
                    }
                    return Ok((r, t));
                }
                0x30 /* '0' */ => {
                    // Consume up to three octal digits; already have one.
                    let mut r = 0;
                    for _ in 1..3 {
                        if t.is_empty() || t.as_bytes()[0] < b'0' || t.as_bytes()[0] > b'7' {
                            break;
                        }
                        r = r * 8 + (t.as_bytes()[0] - b'0') as i32;
                        t = &t[1..];
                    }
                    return Ok((r, t));
                }

                // Hexadecimal escapes.
                0x78 /* 'x' */ => {
                    if t.is_empty() {
                        break 'switch;
                    }
                    let (c, rest) = next_rune(t);
                    t = rest;
                    if c == '{' as i32 {
                        // Any number of digits in braces.
                        let mut nhex = 0;
                        let mut r: i32 = 0;
                        loop {
                            if t.is_empty() {
                                break 'switch;
                            }
                            let (c, rest) = next_rune(t);
                            t = rest;
                            if c == '}' as i32 {
                                break;
                            }
                            let v = unhex(c);
                            if v < 0 {
                                break 'switch;
                            }
                            r = r * 16 + v;
                            if r > MAX_RUNE {
                                break 'switch;
                            }
                            nhex += 1;
                        }
                        if nhex == 0 {
                            break 'switch;
                        }
                        return Ok((r, t));
                    }

                    // Easy case: two hex digits.
                    let x = unhex(c);
                    let (c, rest) = next_rune(t);
                    t = rest;
                    let y = unhex(c);
                    if x < 0 || y < 0 {
                        break 'switch;
                    }
                    return Ok((x * 16 + y, t));
                }

                // C escapes.
                0x61 /* 'a' */ => return Ok((0x07, t)),
                0x66 /* 'f' */ => return Ok((0x0c, t)),
                0x6e /* 'n' */ => return Ok((0x0a, t)),
                0x72 /* 'r' */ => return Ok((0x0d, t)),
                0x74 /* 't' */ => return Ok((0x09, t)),
                0x76 /* 'v' */ => return Ok((0x0b, t)),
                _ => break 'switch,
            }
        }
        Err(self.err(ErrorCode::InvalidEscape, &s[..s.len() - t.len()]))
    }

    /// Go `parseClassChar`: parses a character class character at the
    /// beginning of `s`.
    fn parse_class_char(&self, s: &'a str, whole_class: &str) -> PResult<(i32, &'a str)> {
        if s.is_empty() {
            return Err(self.err(ErrorCode::MissingBracket, whole_class));
        }

        // Allow regular escape sequences even though
        // many need not be escaped in this context.
        if s.as_bytes()[0] == b'\\' {
            return self.parse_escape(s);
        }

        Ok(next_rune(s))
    }

    /// Go `parsePerlClassEscape`: parses a leading Perl character class
    /// escape like `\d` from the beginning of `s`.
    fn parse_perl_class_escape(&mut self, s: &'a str, r: &mut Vec<i32>) -> Option<&'a str> {
        if self.flags & PERL_X == 0 || s.len() < 2 || s.as_bytes()[0] != b'\\' {
            return None;
        }
        let g = perl_group(&s.as_bytes()[..2])?;
        self.append_group(r, g);
        Some(&s[2..])
    }

    /// Go `parseNamedClass`: parses a leading POSIX named character class
    /// like `[:alnum:]` from the beginning of `s`.
    fn parse_named_class(&mut self, s: &'a str, r: &mut Vec<i32>) -> PResult<Option<&'a str>> {
        if s.len() < 2 || s.as_bytes()[0] != b'[' || s.as_bytes()[1] != b':' {
            return Ok(None);
        }

        let Some(i) = s[2..].find(":]") else {
            return Ok(None);
        };
        let i = i + 2;
        let (name, rest) = (&s[..i + 2], &s[i + 2..]);
        let Some(g) = posix_group(name) else {
            return Err(self.err(ErrorCode::InvalidCharRange, name));
        };
        self.append_group(r, g);
        Ok(Some(rest))
    }

    /// Go `appendGroup`.
    fn append_group(&mut self, r: &mut Vec<i32>, g: CharGroup) {
        if self.flags & FOLD_CASE == 0 {
            if g.sign < 0 {
                append_negated_class_local(r, g.class);
            } else {
                append_class(r, g.class);
            }
        } else {
            let mut tmp: Vec<i32> = Vec::new();
            append_folded_class(&mut tmp, g.class);
            clean_class(&mut tmp);
            if g.sign < 0 {
                append_negated_class_local(r, &tmp);
            } else {
                append_class(r, &tmp);
            }
        }
    }

    /// PORT NOTE: Go's `parseUnicodeClass` resolves `\p{...}` / `\P{...}`
    /// via the `unicode` package tables and expands the class into rune
    /// ranges. The tables are not ported; instead the token is validated
    /// syntactically, kept opaque (`Op::UnicodeClass` holding the source
    /// text) and passed through to the regex crate, which resolves the name
    /// with its own Unicode tables when the matcher is compiled. Observable
    /// differences vs Go:
    /// - class-name resolution follows the regex crate (UTS#18 names and its
    ///   Unicode version); the name sets overlap on general categories and
    ///   scripts, but an unknown name fails later, at matcher-compile time
    ///   (`Regex::new`/`PromRegex::new`), with a regex-crate error message
    ///   instead of Go's parse-time "invalid character class range";
    /// - `get_or_values` never expands a Unicode class into or-values, while
    ///   Go expands classes with <= 100 runes (fast-path only; match results
    ///   are identical);
    /// - under `(?i)` the folding is applied by the regex crate at compile
    ///   time instead of Go's parse-time `unicode.SimpleFold` expansion;
    /// - the expanded runes are not counted toward the maxSize/maxRunes
    ///   parse limits (an opaque token counts as size 1).
    ///
    /// `\p{^Name}` is canonicalized to `\P{Name}` (and `\P{^Name}` to
    /// `\p{Name}`) — Go treats them identically and the regex crate does not
    /// accept the caret form.
    ///
    /// Returns `Ok(None)` if `s` does not start with a Unicode class token.
    fn parse_unicode_class_token(&self, s: &'a str) -> PResult<Option<(String, &'a str)>> {
        if self.flags & UNICODE_GROUPS == 0
            || s.len() < 2
            || s.as_bytes()[0] != b'\\'
            || (s.as_bytes()[1] != b'p' && s.as_bytes()[1] != b'P')
        {
            return Ok(None);
        }
        let mut negated = s.as_bytes()[1] == b'P';
        let t = &s[2..];
        if t.is_empty() {
            // Go: the empty name resolves no unicode table → invalid range.
            return Err(self.err(ErrorCode::InvalidCharRange, s));
        }
        let (name, rest, braced) = if t.as_bytes()[0] == b'{' {
            // Name is in braces (Go searches the whole remainder for '}').
            let Some(end) = s.find('}') else {
                return Err(self.err(ErrorCode::InvalidCharRange, s));
            };
            (&s[3..end], &s[end + 1..], true)
        } else {
            // Single-letter name.
            let c = t.chars().next().unwrap();
            let n = c.len_utf8();
            (&t[..n], &t[n..], false)
        };
        let mut name = name;
        if let Some(stripped) = name.strip_prefix('^') {
            negated = !negated;
            name = stripped;
        }
        if name.is_empty() {
            // Go: unicodeTable("") == nil → invalid char range for the seq.
            let seq_len = s.len() - rest.len();
            return Err(self.err(ErrorCode::InvalidCharRange, &s[..seq_len]));
        }
        let sign = if negated { 'P' } else { 'p' };
        let tok = if braced {
            format!("\\{sign}{{{name}}}")
        } else {
            format!("\\{sign}{name}")
        };
        Ok(Some((tok, rest)))
    }

    /// Scans the character class starting at `s` (which begins with `[`)
    /// for an embedded `\p{...}` / `\P{...}` token.
    ///
    /// PORT NOTE: Go merges the resolved Unicode ranges into the containing
    /// class; without the tables, the WHOLE class is kept opaque
    /// (`Op::UnicodeClass` with the `[...]` source text, `\p{^..}`
    /// canonicalized) and resolved by the regex crate at matcher-compile
    /// time — see `parse_unicode_class_token` for the observable
    /// differences. Returns `Ok(None)` when the class contains no Unicode
    /// class token, so the regular (faithful) class parser handles it.
    fn scan_unicode_char_class(&self, s: &'a str) -> PResult<Option<(String, &'a str)>> {
        if self.flags & UNICODE_GROUPS == 0 {
            return Ok(None);
        }
        let bytes = s.as_bytes();
        let mut out = String::from("[");
        let mut i = 1; // chop [
        let mut first = true; // ] is a literal as the first char in the class
        let mut has_unicode = false;
        if i < bytes.len() && bytes[i] == b'^' {
            out.push('^');
            i += 1;
        }
        while i < bytes.len() {
            match bytes[i] {
                b']' if !first => {
                    i += 1;
                    if !has_unicode {
                        return Ok(None);
                    }
                    out.push(']');
                    return Ok(Some((out, &s[i..])));
                }
                b'\\' if i + 1 < bytes.len() && (bytes[i + 1] == b'p' || bytes[i + 1] == b'P') => {
                    let Some((tok, rest)) = self.parse_unicode_class_token(&s[i..])? else {
                        unreachable!("guard checked UNICODE_GROUPS and the \\p prefix");
                    };
                    out.push_str(&tok);
                    i = s.len() - rest.len();
                    has_unicode = true;
                }
                b'\\' => {
                    // Copy the escape pair verbatim (escaped char may be
                    // multi-byte). A trailing lone backslash is left to the
                    // regular class parser to report faithfully.
                    out.push('\\');
                    i += 1;
                    if let Some(ch) = s[i..].chars().next() {
                        out.push(ch);
                        i += ch.len_utf8();
                    }
                }
                b'[' if i + 2 < bytes.len() && bytes[i + 1] == b':' => {
                    // POSIX named class like [:alpha:] — copy it through the
                    // closing ":]" (validated later by whoever parses it).
                    match s[i + 2..].find(":]") {
                        Some(j) => {
                            out.push_str(&s[i..i + 2 + j + 2]);
                            i += 2 + j + 2;
                        }
                        None => {
                            out.push('[');
                            i += 1;
                        }
                    }
                }
                _ => {
                    let ch = s[i..].chars().next().unwrap();
                    out.push(ch);
                    i += ch.len_utf8();
                }
            }
            first = false;
        }
        if !has_unicode {
            // Let the regular class parser report unterminated classes.
            return Ok(None);
        }
        // Go reaches parseClassChar("") for an unterminated class and
        // reports ErrMissingBracket with the whole class text.
        Err(self.err(ErrorCode::MissingBracket, s))
    }

    /// Go `parseClass`: parses a character class at the beginning of `s`
    /// and pushes it onto the parse stack.
    fn parse_class(&mut self, s: &'a str) -> PResult<&'a str> {
        // A class containing a `\p{...}` token cannot be expanded into rune
        // ranges without the Go unicode tables; keep it opaque instead.
        if let Some((tok, rest)) = self.scan_unicode_char_class(s)? {
            let re = self.new_regexp(Op::UnicodeClass);
            self.nodes[re].flags = self.flags;
            self.nodes[re].name = tok;
            self.push(re)?;
            return Ok(rest);
        }

        let mut t = &s[1..]; // chop [
        let re = self.new_regexp(Op::CharClass);
        self.nodes[re].flags = self.flags;

        let mut sign = 1i32;
        if !t.is_empty() && t.as_bytes()[0] == b'^' {
            sign = -1;
            t = &t[1..];

            // If character class does not match \n, add it here,
            // so that negation later will do the right thing.
            if self.flags & CLASS_NL == 0 {
                self.nodes[re].rune.push('\n' as i32);
                self.nodes[re].rune.push('\n' as i32);
            }
        }

        let mut class = std::mem::take(&mut self.nodes[re].rune);
        let mut first = true; // ] and - are okay as first char in class
        while t.is_empty() || t.as_bytes()[0] != b']' || first {
            // POSIX: - is only okay unescaped as first or last in class.
            // Perl: - is okay anywhere.
            if !t.is_empty()
                && t.as_bytes()[0] == b'-'
                && self.flags & PERL_X == 0
                && !first
                && (t.len() == 1 || t.as_bytes()[1] != b']')
            {
                let size = t[1..].chars().next().map_or(0, |c| c.len_utf8());
                return Err(self.err(ErrorCode::InvalidCharRange, &t[..1 + size]));
            }
            first = false;

            // Look for POSIX [:alnum:] etc.
            if t.len() > 2
                && t.as_bytes()[0] == b'['
                && t.as_bytes()[1] == b':'
                && let Some(nt) = self.parse_named_class(t, &mut class)?
            {
                t = nt;
                continue;
            }

            // Look for Perl character class symbols (extension).
            // (Unicode groups like \p{Han} were handled by the whole-class
            // scan above.)
            if let Some(nt) = self.parse_perl_class_escape(t, &mut class) {
                t = nt;
                continue;
            }

            // Single character or simple range.
            let rng = t;
            let (lo, rest) = self.parse_class_char(t, s)?;
            t = rest;
            let mut hi = lo;
            // [a-] means (a|-) so check for final ].
            if t.len() >= 2 && t.as_bytes()[0] == b'-' && t.as_bytes()[1] != b']' {
                t = &t[1..];
                let (h, rest) = self.parse_class_char(t, s)?;
                t = rest;
                hi = h;
                if hi < lo {
                    let rng = &rng[..rng.len() - t.len()];
                    return Err(self.err(ErrorCode::InvalidCharRange, rng));
                }
            }
            if self.flags & FOLD_CASE == 0 {
                append_range(&mut class, lo, hi);
            } else {
                append_folded_range(&mut class, lo, hi);
            }
        }
        t = &t[1..]; // chop ]

        clean_class(&mut class);
        if sign < 0 {
            negate_class(&mut class);
        }
        self.nodes[re].rune = class;
        self.push(re)?;
        Ok(t)
    }

    /// Exports the arena node `id` as an owned tree.
    fn export(&self, id: usize) -> Regexp {
        let n = &self.nodes[id];
        Regexp {
            op: n.op(),
            flags: n.flags,
            sub: n.sub.iter().map(|&s| self.export(s)).collect(),
            rune: n.rune.clone(),
            min: n.min,
            max: n.max,
            cap: n.cap,
            name: n.name.clone(),
        }
    }
}

// Free-standing alias to keep call sites close to the Go names.
fn append_negated_class_local(r: &mut Vec<i32>, x: &[i32]) {
    super::goclass::append_negated_class(r, x);
}

/// Go `syntax.Parse`: parses a regular expression string `s`, controlled by
/// the specified flags, and returns a regular expression parse tree.
pub fn parse(s: &str, flags: u16) -> Result<Regexp, Error> {
    if flags & LITERAL != 0 {
        // Trivial parser for literal string.
        let mut re = Regexp::new(Op::Literal);
        re.flags = flags;
        re.rune = s.chars().map(|c| c as i32).collect();
        return Ok(re);
    }

    // Otherwise, must do real work.
    let mut p = Parser::new(s, flags);
    let mut t = s;
    let mut last_repeat = "";
    while !t.is_empty() {
        let mut repeat = "";
        'big_switch: {
            match t.as_bytes()[0] {
                b'(' => {
                    if p.flags & PERL_X != 0 && t.len() >= 2 && t.as_bytes()[1] == b'?' {
                        // Flag changes and non-capturing groups.
                        t = p.parse_perl_flags(t)?;
                        break 'big_switch;
                    }
                    p.num_cap += 1;
                    let re = p.op(Op::PseudoLeftParen)?;
                    p.nodes[re].cap = p.num_cap;
                    t = &t[1..];
                }
                b'|' => {
                    p.parse_vertical_bar()?;
                    t = &t[1..];
                }
                b')' => {
                    p.parse_right_paren()?;
                    t = &t[1..];
                }
                b'^' => {
                    if p.flags & ONE_LINE != 0 {
                        p.op(Op::BeginText)?;
                    } else {
                        p.op(Op::BeginLine)?;
                    }
                    t = &t[1..];
                }
                b'$' => {
                    if p.flags & ONE_LINE != 0 {
                        let re = p.op(Op::EndText)?;
                        p.nodes[re].flags |= WAS_DOLLAR;
                    } else {
                        p.op(Op::EndLine)?;
                    }
                    t = &t[1..];
                }
                b'.' => {
                    if p.flags & DOT_NL != 0 {
                        p.op(Op::AnyChar)?;
                    } else {
                        p.op(Op::AnyCharNotNL)?;
                    }
                    t = &t[1..];
                }
                b'[' => {
                    t = p.parse_class(t)?;
                }
                b'*' | b'+' | b'?' => {
                    let before = t;
                    let op = match t.as_bytes()[0] {
                        b'*' => Op::Star,
                        b'+' => Op::Plus,
                        _ => Op::Quest,
                    };
                    let after = &t[1..];
                    let after = p.repeat(op, 0, 0, before, after, last_repeat)?;
                    repeat = before;
                    t = after;
                }
                b'{' => {
                    let before = t;
                    match p.parse_repeat(t) {
                        None => {
                            // If the repeat cannot be parsed, { is a literal.
                            p.literal('{' as i32)?;
                            t = &t[1..];
                        }
                        Some((min, max, after)) => {
                            if !(0..=1000).contains(&min) || max > 1000 || (max >= 0 && min > max) {
                                // Numbers were too big, or max is present and min > max.
                                return Err(p.err(
                                    ErrorCode::InvalidRepeatSize,
                                    &before[..before.len() - after.len()],
                                ));
                            }
                            let after =
                                p.repeat(Op::Repeat, min, max, before, after, last_repeat)?;
                            repeat = before;
                            t = after;
                        }
                    }
                }
                b'\\' => {
                    if p.flags & PERL_X != 0 && t.len() >= 2 {
                        match t.as_bytes()[1] {
                            b'A' => {
                                p.op(Op::BeginText)?;
                                t = &t[2..];
                                break 'big_switch;
                            }
                            b'b' => {
                                p.op(Op::WordBoundary)?;
                                t = &t[2..];
                                break 'big_switch;
                            }
                            b'B' => {
                                p.op(Op::NoWordBoundary)?;
                                t = &t[2..];
                                break 'big_switch;
                            }
                            b'C' => {
                                // any byte; not supported
                                return Err(p.err(ErrorCode::InvalidEscape, &t[..2]));
                            }
                            b'Q' => {
                                // \Q ... \E: the ... is always literals
                                let (mut lit, rest) = match t[2..].find(r"\E") {
                                    Some(i) => (&t[2..2 + i], &t[2 + i + 2..]),
                                    None => (&t[2..], ""),
                                };
                                while !lit.is_empty() {
                                    let (c, rest2) = next_rune(lit);
                                    p.literal(c)?;
                                    lit = rest2;
                                }
                                t = rest;
                                break 'big_switch;
                            }
                            b'z' => {
                                p.op(Op::EndText)?;
                                t = &t[2..];
                                break 'big_switch;
                            }
                            _ => {}
                        }
                    }

                    // Look for Unicode character group like \p{Han}
                    if let Some((tok, rest)) = p.parse_unicode_class_token(t)? {
                        let re = p.new_regexp(Op::UnicodeClass);
                        p.nodes[re].flags = p.flags;
                        p.nodes[re].name = tok;
                        t = rest;
                        p.push(re)?;
                        break 'big_switch;
                    }

                    // Perl character class escape.
                    let re = p.new_regexp(Op::CharClass);
                    p.nodes[re].flags = p.flags;
                    let mut class: Vec<i32> = Vec::new();
                    if let Some(rest) = p.parse_perl_class_escape(t, &mut class) {
                        p.nodes[re].rune = class;
                        t = rest;
                        p.push(re)?;
                        break 'big_switch;
                    }
                    p.reuse(re);

                    // Ordinary single-character escape.
                    let (c, rest) = p.parse_escape(t)?;
                    t = rest;
                    p.literal(c)?;
                }
                _ => {
                    let (c, rest) = next_rune(t);
                    t = rest;
                    p.literal(c)?;
                }
            }
        }
        last_repeat = repeat;
    }

    p.concat()?;
    if p.swap_vertical_bar() {
        // pop vertical bar
        p.stack.pop();
    }
    p.alternate()?;

    if p.stack.len() != 1 {
        return Err(Error {
            code: ErrorCode::MissingParen,
            expr: s.to_string(),
        });
    }
    Ok(p.export(p.stack[0]))
}
