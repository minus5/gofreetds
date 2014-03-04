package freetds

import (
	"strings"
	"unicode"
)

//This code is stolen from:
//https://bitbucket.org/pkg/inflect/src/8961c3750a47b8c0b3e118d52513b97adf85a7e8/inflect.go

// "dino_party" -> "DinoParty"
func camelize(word string) string {
	words := splitAtCaseChangeWithTitlecase(word)
	return strings.Join(words, "")
}

func isSpacerChar(c rune) bool {
	switch {
	case c == rune("_"[0]):
		return true
	case c == rune(" "[0]):
		return true
	case c == rune(":"[0]):
		return true
	case c == rune("-"[0]):
		return true
	}
	return false
}

func splitAtCaseChangeWithTitlecase(s string) []string {
	words := make([]string, 0)
	word := make([]rune, 0)
	for _, c := range s {
		spacer := isSpacerChar(c)
		if len(word) > 0 {
			if unicode.IsUpper(c) || spacer {
				words = append(words, string(word))
				word = make([]rune, 0)
			}
		}
		if !spacer {
			if len(word) > 0 {
				word = append(word, unicode.ToLower(c))
			} else {
				word = append(word, unicode.ToUpper(c))
			}
		}
	}
	words = append(words, string(word))
	return words
}
