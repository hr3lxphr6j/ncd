use std::path::Path;

pub struct FileNameUtils;

impl FileNameUtils {
    /// 不正なファイル名文字を全角文字に置換
    pub fn replace_illegal_filename_characters(input: &str) -> String {
        input
            .chars()
            .map(|c| match c {
                '"' => '＂',  // U+FF02
                '*' => '＊',  // U+FF0A
                '/' => '／',  // U+FF0F
                ':' => '：',  // U+FF1A
                '<' => '＜',  // U+FF1C
                '>' => '＞',  // U+FF1E
                '?' => '？',  // U+FF1F
                '\\' => '＼', // U+FF3C
                '|' => '￨',   // U+FFE8
                '\t' | '\n' | '\r' | '\u{0b}' | '\u{0c}' => ' ',
                _ => c,
            })
            .collect()
    }

    /// UTF-8 バイト長で文字列を切り詰め、文字境界を破壊しない
    pub fn truncate_str_by_byte_len(s: &str, max_len: usize) -> String {
        let bytes = s.as_bytes();

        if bytes.len() <= max_len {
            return s.to_string();
        }

        let mut end = max_len;

        // UTF-8 文字の開始位置まで戻る
        while end > 0 && (bytes[end] & 0b1100_0000) == 0b1000_0000 {
            end -= 1;
        }

        String::from_utf8_lossy(&bytes[..end]).to_string()
    }

    /// ファイル名を切り詰め（拡張子を保持）
    pub fn truncate_filename(filename: &str, max_file_length: usize, suffix: &str) -> String {
        let path = Path::new(filename);

        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| format!(".{}", e))
            .unwrap_or_default();

        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");

        let max_base_len = max_file_length.saturating_sub(ext.len() + suffix.len());

        let truncated_stem = Self::truncate_str_by_byte_len(stem, max_base_len);

        format!("{}{}{}", truncated_stem, suffix, ext)
    }
}
