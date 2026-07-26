//! Repository-local ignore rules used by both `add` and `status`.
//!
//! The format intentionally follows the useful, portable subset of Git's ignore
//! syntax: blank lines and comments are ignored, `!` negates a rule, a trailing
//! `/` limits a rule to directories, `*` and `?` match within one path component,
//! and `**` matches across components.  The file is read only from the repository
//! root (`.neoengramignore`); nested ignore files are not currently supported.

use std::{collections::HashMap, fs, path::Path};

use anyhow::{Context, Result};

const IGNORE_FILE_NAME: &str = ".neoengramignore";

#[derive(Clone, Debug, Default)]
pub(crate) struct IgnoreRules {
    patterns: Vec<IgnorePattern>,
}

#[derive(Clone, Debug)]
struct IgnorePattern {
    negative: bool,
    directory_only: bool,
    anchored: bool,
    /// A pattern containing a slash is matched against a repository-rooted path;
    /// a pattern without a slash is matched against every path component.
    has_slash: bool,
    components: Vec<String>,
}

impl IgnoreRules {
    pub(crate) fn load(repository_root: &Path) -> Result<Self> {
        let path = repository_root.join(IGNORE_FILE_NAME);
        let contents = match fs::read(&path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default())
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("无法读取忽略规则文件: {}", path.display()));
            }
        };
        let contents = String::from_utf8(contents)
            .with_context(|| format!("忽略规则文件不是有效 UTF-8: {}", path.display()))?;
        Ok(Self::from_str(&contents))
    }

    pub(crate) fn from_str(contents: &str) -> Self {
        let mut patterns = Vec::new();
        for raw_line in contents.lines() {
            let mut line = raw_line
                .strip_suffix('\r')
                .unwrap_or(raw_line)
                .trim()
                .to_owned();
            if line.is_empty() {
                continue;
            }

            // A leading backslash quotes a comment/negation marker.  Other
            // backslashes are retained and treated as literal characters by the
            // component matcher.
            let escaped_marker = line.starts_with("\\#") || line.starts_with("\\!");
            if line.starts_with('#') && !escaped_marker {
                continue;
            }

            let negative = line.starts_with('!') && !escaped_marker;
            if negative || escaped_marker {
                line.remove(0);
            }

            let directory_only = line.ends_with('/') && !line.ends_with("\\/");
            if directory_only {
                line.pop();
            }

            // A leading slash anchors a slash-containing pattern at the
            // repository root.  Since this file is root-only, unanchored
            // slash-containing patterns are also root-relative; slashless rules
            // retain Git's basename-at-any-depth behavior.
            let anchored = line.starts_with('/');
            if anchored {
                line.remove(0);
            }
            let components: Vec<String> = line
                .split('/')
                .filter(|component| !component.is_empty())
                .map(str::to_owned)
                .collect();
            if components.is_empty() {
                continue;
            }
            patterns.push(IgnorePattern {
                negative,
                directory_only,
                anchored,
                has_slash: components.len() > 1,
                components,
            });
        }
        Self { patterns }
    }

    /// Returns whether a repository-relative path is ignored after applying all
    /// rules in order.  Tracked paths are handled by callers separately; this
    /// predicate only controls discovery of new files.
    pub(crate) fn is_ignored(&self, repository_path: &str, is_dir: bool) -> bool {
        let components: Vec<&str> = repository_path.split('/').collect();
        let mut ignored = false;
        for pattern in &self.patterns {
            if pattern.matches(&components, is_dir) {
                ignored = !pattern.negative;
            }
        }
        ignored
    }

    pub(crate) fn has_negations(&self) -> bool {
        self.patterns.iter().any(|pattern| pattern.negative)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.patterns.len()
    }
}

impl IgnorePattern {
    fn matches(&self, path: &[&str], is_dir: bool) -> bool {
        // A directory-only rule applies to the directory itself and to every
        // descendant, but never to a same-named regular file.
        let last_prefix = if self.directory_only && !is_dir {
            path.len().saturating_sub(1)
        } else {
            path.len()
        };
        if last_prefix == 0 {
            return false;
        }

        for end in 1..=last_prefix {
            let candidate = &path[..end];
            if self.matches_candidate(candidate) {
                return true;
            }
        }
        false
    }

    fn matches_candidate(&self, candidate: &[&str]) -> bool {
        if !self.has_slash && !self.anchored {
            // A slashless rule matches a basename at any depth.  It also
            // matches a directory basename so descendants inherit the rule.
            return candidate
                .last()
                .is_some_and(|name| segment_matches(&self.components[0], name));
        }
        glob_components(&self.components, candidate)
    }
}

fn glob_components(pattern: &[String], path: &[&str]) -> bool {
    fn visit(
        pattern: &[String],
        path: &[&str],
        pattern_index: usize,
        path_index: usize,
        memo: &mut HashMap<(usize, usize), bool>,
    ) -> bool {
        if let Some(result) = memo.get(&(pattern_index, path_index)) {
            return *result;
        }
        let result = if pattern_index == pattern.len() {
            path_index == path.len()
        } else if pattern[pattern_index] == "**" {
            visit(pattern, path, pattern_index + 1, path_index, memo)
                || (path_index < path.len()
                    && visit(pattern, path, pattern_index, path_index + 1, memo))
        } else {
            path_index < path.len()
                && segment_matches(&pattern[pattern_index], path[path_index])
                && visit(pattern, path, pattern_index + 1, path_index + 1, memo)
        };
        memo.insert((pattern_index, path_index), result);
        result
    }

    visit(pattern, path, 0, 0, &mut HashMap::new())
}

fn segment_matches(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    let mut previous = vec![false; text.len() + 1];
    previous[0] = true;

    let mut index = 0;
    while index < pattern.len() {
        let marker = pattern[index];
        let mut current = vec![false; text.len() + 1];
        match marker {
            '*' => {
                current[0] = previous[0];
                for text_index in 1..=text.len() {
                    current[text_index] = previous[text_index] || current[text_index - 1];
                }
            }
            '?' => {
                current[1..].copy_from_slice(&previous[..text.len()]);
            }
            '\\' if index + 1 < pattern.len() => {
                let literal = pattern[index + 1];
                for text_index in 1..=text.len() {
                    current[text_index] =
                        previous[text_index - 1] && text[text_index - 1] == literal;
                }
                index += 1;
            }
            literal => {
                for text_index in 1..=text.len() {
                    current[text_index] =
                        previous[text_index - 1] && text[text_index - 1] == literal;
                }
            }
        }
        previous = current;
        index += 1;
    }
    previous[text.len()]
}

#[cfg(test)]
mod tests {
    use super::IgnoreRules;

    #[test]
    fn parses_comments_negations_and_directory_rules() {
        let rules = IgnoreRules::from_str("# comment\n*.bin\n!keep.bin\ncache/\n");
        assert_eq!(rules.len(), 3);
        assert!(rules.is_ignored("weights.bin", false));
        assert!(!rules.is_ignored("keep.bin", false));
        assert!(rules.is_ignored("nested/weights.bin", false));
        assert!(rules.is_ignored("cache/model.pt", false));
        assert!(!rules.is_ignored("cache.pt", false));
    }

    #[test]
    fn slash_patterns_and_double_star_match_expected_paths() {
        let rules = IgnoreRules::from_str("/build/*.tmp\nlogs/**/trace.log\n");
        assert!(rules.is_ignored("build/a.tmp", false));
        assert!(!rules.is_ignored("nested/build/a.tmp", false));
        assert!(rules.is_ignored("logs/trace.log", false));
        assert!(rules.is_ignored("logs/a/b/trace.log", false));
        assert!(!rules.is_ignored("other/trace.log", false));
    }

    #[test]
    fn slashless_patterns_match_directory_descendants() {
        let rules = IgnoreRules::from_str("cache\n");
        assert!(rules.is_ignored("cache/model.bin", false));
        assert!(rules.is_ignored("nested/cache/model.bin", false));
        assert!(!rules.is_ignored("cached/model.bin", false));
    }

    #[test]
    fn leading_slash_anchors_single_component_rules() {
        let rules = IgnoreRules::from_str("/build/\n");
        assert!(rules.is_ignored("build/model.bin", false));
        assert!(!rules.is_ignored("nested/build/model.bin", false));
    }
}
