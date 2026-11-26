package adapter

import (
	"encoding/json"
	"regexp"
	"strings"
)

// Markup pairs the REST API's JSON representation with Markdown.
type Markup interface {
	ToMarkdown(json.RawMessage) (string, error)
	FromMarkdown(string) (json.RawMessage, error)
}

type wikiMarkup struct{}

var (
	wikiHeading  = regexp.MustCompile(`^h([1-6])\. +(.+)$`)
	mdHeading    = regexp.MustCompile(`^(#{1,6}) +(.+)$`)
	wikiList     = regexp.MustCompile(`^([*#]+) +(.+)$`)
	mdList       = regexp.MustCompile(`^([ \t]*)([-+*]|[0-9]+\.)[ \t]+(.+)$`)
	wikiCode     = regexp.MustCompile(`^\{(code(?::[^}]*)?|noformat)\}$`)
	mdFence      = regexp.MustCompile("^ {0,3}(`{3,}|~{3,})(.*)$")
	fenceRuns    = regexp.MustCompile("`+")
	tableDivider = regexp.MustCompile(`^:?-{3,}:?$`)
	wikiInline   = regexp.MustCompile(`\{\{([^{}\n]+)\}\}|\[~([^]\n]+)\]|\[([^|\]\n]+)\|([^\]\n]+)\]|\[(https?://[^\]\n]+)\]|(\*[^*\s](?:[^*\n]*[^*\s])?\*)|(_[^_\s](?:[^_\n]*[^_\s])?_)`)
	mdInline     = regexp.MustCompile("`+[^`\n]+`+|\\[[^\\]\n]+\\]\\([^()\n]+\\)|\\*\\*[^*\n]+\\*\\*|__[^_\n]+__|\\*[^*\n]+\\*|_[^_\n]+_|@[A-Za-z0-9_][A-Za-z0-9_.:-]*")
)

func (wikiMarkup) ToMarkdown(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	var wiki string
	if err := json.Unmarshal(raw, &wiki); err != nil {
		return "", err
	}
	lines := splitLines(wiki)
	var out []string
	quoted := false
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		switch {
		case line == "{quote}":
			quoted = !quoted
			continue
		case wikiCode.MatchString(line):
			open := wikiCode.FindStringSubmatch(line)[1]
			kind, params, _ := strings.Cut(open, ":")
			closeTag := "{" + kind + "}"
			end := i + 1
			for end < len(lines) && lines[end] != closeTag {
				end++
			}
			if end < len(lines) {
				language, _, _ := strings.Cut(params, "|")
				language = strings.TrimPrefix(language, "language=")
				code := strings.Join(lines[i+1:end], "\n")
				fence := codeFence(code, 3)
				for _, codeLine := range append([]string{fence + language}, append(lines[i+1:end:end], fence)...) {
					out = append(out, quoteLine(codeLine, quoted))
				}
				i = end
				continue
			}
		case wikiHeading.MatchString(line):
			m := wikiHeading.FindStringSubmatch(line)
			line = strings.Repeat("#", int(m[1][0]-'0')) + " " + inlineMarkdown(m[2])
			out = append(out, quoteLine(line, quoted))
			continue
		case wikiList.MatchString(line):
			m := wikiList.FindStringSubmatch(line)
			var indent strings.Builder
			for _, marker := range m[1][:len(m[1])-1] {
				indent.WriteString("  ")
				if marker == '#' {
					indent.WriteByte(' ')
				}
			}
			marker := "- "
			if strings.HasSuffix(m[1], "#") {
				marker = "1. "
			}
			out = append(out, quoteLine(indent.String()+marker+inlineMarkdown(m[2]), quoted))
			continue
		case strings.HasPrefix(line, "bq. "):
			out = append(out, quoteLine("> "+inlineMarkdown(line[4:]), quoted))
			continue
		case strings.HasPrefix(line, "||") && strings.HasSuffix(line, "||") && len(line) > 4:
			cells := tableCells(line, "||")
			out = append(out, quoteLine(tableLine(cells, inlineMarkdown, "|"), quoted))
			for i := range cells {
				cells[i] = "---"
			}
			out = append(out, quoteLine(tableLine(cells, identity, "|"), quoted))
			continue
		case isTableRow(line):
			out = append(out, quoteLine(tableLine(tableCells(line, "|"), inlineMarkdown, "|"), quoted))
			continue
		}
		out = append(out, quoteLine(inlineMarkdown(line), quoted))
	}
	return strings.Join(out, "\n"), nil
}

func (wikiMarkup) FromMarkdown(md string) (json.RawMessage, error) {
	return json.Marshal(strings.Join(toWikiLines(splitLines(md)), "\n"))
}

type listLevel struct {
	indent int
	marker byte
}

func toWikiLines(lines []string) []string {
	var out []string
	var list []listLevel
	inTable := false
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if m := mdList.FindStringSubmatch(line); m != nil {
			indent := len(strings.ReplaceAll(m[1], "\t", "    "))
			for len(list) > 0 && indent <= list[len(list)-1].indent {
				list = list[:len(list)-1]
			}
			marker := byte('*')
			if strings.HasSuffix(m[2], ".") {
				marker = '#'
			}
			list = append(list, listLevel{indent, marker})
			var prefix strings.Builder
			for _, level := range list {
				prefix.WriteByte(level.marker)
			}
			out = append(out, prefix.String()+" "+inlineWiki(m[3]))
			inTable = false
			continue
		}
		if strings.TrimSpace(line) != "" {
			list = nil
		}
		if m := mdFence.FindStringSubmatch(line); m != nil {
			end := i + 1
			for end < len(lines) && !closesFence(lines[end], m[1]) {
				end++
			}
			if end < len(lines) {
				tag := "{code}"
				if lang := strings.Fields(m[2]); len(lang) > 0 && !strings.ContainsAny(lang[0], "{}|") {
					tag = "{code:" + lang[0] + "}"
				}
				out = append(out, tag)
				out = append(out, lines[i+1:end]...)
				out = append(out, "{code}")
				i = end
				inTable = false
				continue
			}
		}
		if isQuote(line) {
			var quote []string
			for i < len(lines) && isQuote(lines[i]) {
				quote = append(quote, strings.TrimPrefix(strings.TrimSpace(lines[i])[1:], " "))
				i++
			}
			i--
			out = append(out, "{quote}")
			out = append(out, toWikiLines(quote)...)
			out = append(out, "{quote}")
			inTable = false
			continue
		}
		if isTableRow(line) {
			if i+1 < len(lines) && isDivider(lines[i+1]) {
				out = append(out, tableLine(tableCells(line, "|"), inlineWiki, "||"))
				i++
				inTable = true
				continue
			}
			if inTable {
				out = append(out, tableLine(tableCells(line, "|"), inlineWiki, "|"))
				continue
			}
		}
		inTable = false
		if m := mdHeading.FindStringSubmatch(line); m != nil {
			out = append(out, "h"+string(rune('0'+len(m[1])))+". "+inlineWiki(m[2]))
		} else {
			out = append(out, inlineWiki(line))
		}
	}
	return out
}

func splitLines(text string) []string {
	return strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
}

func quoteLine(line string, quoted bool) string {
	if quoted {
		return "> " + line
	}
	return line
}

func isQuote(line string) bool {
	return strings.HasPrefix(strings.TrimSpace(line), ">")
}

func closesFence(line, fence string) bool {
	line = strings.TrimSpace(line)
	return strings.HasPrefix(line, fence) && strings.Trim(line, fence[:1]) == ""
}

func codeFence(code string, minLength int) string {
	for _, run := range fenceRuns.FindAllString(code, -1) {
		minLength = max(minLength, len(run)+1)
	}
	return strings.Repeat("`", minLength)
}

func isTableRow(line string) bool {
	return len(line) > 2 && strings.HasPrefix(line, "|") && strings.HasSuffix(line, "|")
}

func tableCells(line, sep string) []string {
	line = strings.TrimSuffix(strings.TrimPrefix(line, sep), sep)
	var cells []string
	start := 0
	for i := 0; i < len(line); {
		if line[i] == '\\' && i+1 < len(line) {
			i += 2
			continue
		}
		if strings.HasPrefix(line[i:], sep) {
			cells = append(cells, strings.TrimSpace(line[start:i]))
			i += len(sep)
			start = i
			continue
		}
		i++
	}
	return append(cells, strings.TrimSpace(line[start:]))
}

func identity(s string) string { return s }

func tableLine(cells []string, convert func(string) string, sep string) string {
	var result []string
	for _, cell := range cells {
		result = append(result, convert(cell))
	}
	return sep + " " + strings.Join(result, " "+sep+" ") + " " + sep
}

func isDivider(line string) bool {
	if !isTableRow(line) {
		return false
	}
	for _, cell := range tableCells(line, "|") {
		if !tableDivider.MatchString(cell) {
			return false
		}
	}
	return true
}

func inlineMarkdown(text string) string {
	return wikiInline.ReplaceAllStringFunc(text, func(token string) string {
		match := wikiInline.FindStringSubmatch(token)
		switch {
		case match[1] != "":
			fence := codeFence(match[1], 1)
			code := match[1]
			if strings.HasPrefix(code, "`") || strings.HasSuffix(code, "`") || strings.TrimSpace(code) != code {
				code = " " + code + " "
			}
			return fence + code + fence
		case match[2] != "":
			return "@" + match[2]
		case match[3] != "":
			return "[" + match[3] + "](" + match[4] + ")"
		case match[5] != "":
			return "<" + match[5] + ">"
		case match[6] != "":
			return "*" + token + "*"
		default:
			return token
		}
	})
}

func inlineWiki(text string) string {
	var out strings.Builder
	pos := 0
	for _, m := range mdInline.FindAllStringIndex(text, -1) {
		start, end := m[0], m[1]
		out.WriteString(text[pos:start])
		token := text[start:end]
		escaped := start > 0 && text[start-1] == '\\'
		if !escaped {
			switch {
			case token[0] == '`':
				fence := fenceRuns.FindString(token)
				code, closed := strings.CutSuffix(token[len(fence):], fence)
				if closed {
					if strings.HasPrefix(code, " ") && strings.HasSuffix(code, " ") && strings.TrimSpace(code) != "" {
						code = code[1 : len(code)-1]
					}
					token = "{{" + code + "}}"
				}
			case token[0] == '[':
				label, target, _ := strings.Cut(token[1:len(token)-1], "](")
				token = "[" + label + "|" + target + "]"
			case strings.HasPrefix(token, "**") || strings.HasPrefix(token, "__"):
				token = "*" + token[2:len(token)-2] + "*"
			case token[0] == '*':
				token = "_" + token[1:len(token)-1] + "_"
			case token[0] == '@':
				if start == 0 || strings.ContainsRune(" \t([{>\"", rune(text[start-1])) {
					mention := strings.TrimRight(token, ".:-")
					token = "[~" + mention[1:] + "]" + token[len(mention):]
				}
			}
		}
		out.WriteString(token)
		pos = end
	}
	out.WriteString(text[pos:])
	return out.String()
}
