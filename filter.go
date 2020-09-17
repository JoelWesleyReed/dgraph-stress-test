package main

import "strings"

var replacer *strings.Replacer

func init() {
	replacer = strings.NewReplacer(
		string(byte(0x08)), " ", // backspace
		string(byte(0x0C)), " ", // form feed
		string(byte(0x0A)), " ", // new line
		string(byte(0x0D)), " ", // carriage return
		string(byte(0x09)), " ", // tab
		`\b`, " ",
		`\f`, " ",
		`\n`, " ",
		`\r`, " ",
		`\t`, " ",
		`\"`, " ",
		`^`, "",
		`{`, "",
		`}`, "",
		"`", "",
		`~`, "",
		`\`, "",
		`"`, "",
	)
}

func removeInvalidChars(s string) string {
	s = replacer.Replace(s)
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "_:")
	s = strings.TrimSpace(s)
	return s
}
