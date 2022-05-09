package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func parse(s string) interface{} {
	symbolRunes := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+*/-_!=<>"
	pos := -1
	i := []interface{}{}
	a := []int{}
	lastAPos := 0
	next := func() byte {
		pos++
		if pos >= len(s) {
			return 0
		}
		return s[pos]
	}
	for {
		start := pos + 1
		switch c := next(); {
		case c == 0:
			if len(a) > 0 {
				panic(fmt.Sprintf("unclosed parens starting at position %d", lastAPos))
			}
			if len(i) == 0 {
				return nil
			}
			if len(i) == 1 {
				return i[0]
			}
			return []interface{}{append([]interface{}{"fn", nil}, i...)}
		case strings.ContainsRune(" \t\n\r", rune(c)):
			// ignore
		case c == '(':
			lastAPos = pos
			a = append(a, len(i))
		case c == ')':
			if len(a) == 0 {
				panic(fmt.Sprintf("unexpected extra closing parens at position %d", pos))
			}
			start := a[len(a)-1]
			l := append([]interface{}{}, i[start:]...)
			if len(l) == 0 {
				i = append(i[0:start], nil)
			} else {
				i = append(i[0:start], l)
			}
			a = a[:len(a)-1]
		case (c >= '0' && c <= '9') || c == '-':
			for c = next(); c >= '0' && c <= '9'; c = next() {
			}
			n, err := strconv.ParseInt(s[start:pos], 10, 64)
			check(err)
			i = append(i, n)
			pos--
		case strings.ContainsRune(symbolRunes, rune(c)):
			for c = next(); strings.ContainsRune(symbolRunes, rune(c)); c = next() {
			}
			i = append(i, s[start:pos])
			pos--
		default:
			panic(fmt.Sprintf("unexpected character at position %d", pos))
		}
	}
}

func eval(env map[string]interface{}, a interface{}) interface{} {
	switch b := a.(type) {
	case nil:
		return a
	case int64:
		return a
	case string:
		if c, ok := envGet(env, b); ok {
			return c
		}
		panic(fmt.Sprintf("eval: no '%s' in env", b))
	case []interface{}:
		if bi, ok := b[0].(string); ok {
			switch bi {
			case "+":
				values := mustArgs("+", env, b[1:], "number", "number")
				return mustNumber(values[0]) + mustNumber(values[1])
			case "-":
				values := mustArgs("-", env, b[1:], "number", "number")
				return mustNumber(values[0]) - mustNumber(values[1])
			case "quote":
				return b[1]
			case "def":
				values := mustArgs("def", env, b[2:], "string")
				env[mustString(b[1])] = values[0]
				return values[0]
			case "fn":
				mustList(b[1])
				return append([]interface{}{env}, b[1:]...)
			case "cond":
				for _, c := range b[1:] {
					cc := mustList(c)
					if eval(env, cc[0]) != nil {
						return eval(env, cc[1])
					}
				}
				return nil
			case "eq":
				values := mustArgs("eq", env, b[1:])
				if print(values[0]) == print(values[1]) {
					return "t"
				}
				return nil
			case "type":
				v := eval(env, b[1])
				switch v.(type) {
				case nil:
					return "list"
				case int64:
					return "number"
				case string:
					return "symbol"
				case []interface{}:
					return "list"
				case map[string]interface{}:
					return "env"
				}
			case "list":
				return mustArgs("list", env, b[1:])
			case "concat":
				values := mustArgs("concat", env, b[1:])
				d := []interface{}{}
				for _, v := range values {
					if vv, ok := v.([]interface{}); ok {
						d = append(d, vv...)
					} else {
						d = append(d, v)
					}
				}
				return d
			case "nth":
				values := mustArgs("nth", env, b[1:], "list", "number")
				l := mustList(values[0])
				n := int(mustNumber(values[1]))
				if n >= len(l) {
					if len(values) >= 3 {
						return values[2]
					} else {
						return nil
					}
				}
				return l[n]
			default:
			}
		}
		fnRaw := eval(env, b[0])
		fn, ok := fnRaw.([]interface{})
		if !ok {
			panic(fmt.Sprintf("eval: call to non fn: %v", fnRaw))
		}
		fnDefEnv, ok := fn[0].(map[string]interface{})
		if !ok {
			panic(fmt.Sprintf("eval: call to non fn (env): %v", fn))
		}
		argNames, ok := fn[1].([]interface{})
		if fn[1] != nil && !ok {
			panic(fmt.Sprintf("eval: call to non fn (args): %v", fn))
		}
		fnEnv := map[string]interface{}{"*up*": fnDefEnv}
		args := []interface{}{}
		for _, c := range b[1:] {
			args = append(args, eval(env, c))
		}
		if argNames == nil {
			fnEnv["args"] = args
		} else {
			for i, a := range argNames {
				b := mustString(a)
				fnEnv[b] = args[i]
			}
		}
		var d interface{}
		for _, e := range fn[2:] {
			d = eval(fnEnv, e)
		}
		return d
	default:
		panic(fmt.Sprintf("eval: unknown value type: %T %v", a, a))
	}
}

func print(a interface{}) string {
	switch b := a.(type) {
	case nil:
		return "()"
	case int64:
		return fmt.Sprintf("%d", b)
	case string:
		return b
	case []interface{}:
		cs := []string{}
		for _, c := range b {
			cs = append(cs, print(c))
		}
		return "(" + strings.Join(cs, " ") + ")"
	case map[string]interface{}:
		return fmt.Sprintf("<env %p>", b)
	default:
		panic(fmt.Sprintf("print: unknown value type: %v", a))
	}
}

func main() {
	env := map[string]interface{}{}
	eval(env, parse(stdlib))
	reader := bufio.NewReader(os.Stdin)
	repl := func() {
		for {
			fmt.Print("> ")
			line, _, err := reader.ReadLine()
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			fmt.Println(print(eval(env, parse(string(line)))))
		}
	}
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("error: %v\n", err)
			repl()
		}
	}()
	repl()
}

func envGet(env map[string]interface{}, name string) (interface{}, bool) {
	if a, ok := env[name]; ok {
		return a, true
	}
	if p, ok := env["*up*"]; ok {
		if pp, ok := p.(map[string]interface{}); ok {
			return envGet(pp, name)
		}
	}
	return nil, false
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func mustNumber(a interface{}) int64 {
	if b, ok := a.(int64); ok {
		return b
	}
	panic(fmt.Sprintf("expected number got '%v'", a))
}

func mustString(a interface{}) string {
	if b, ok := a.(string); ok {
		return b
	}
	panic(fmt.Sprintf("expected string got '%v'", a))
}

func mustList(a interface{}) []interface{} {
	if a == nil {
		return nil
	}
	if b, ok := a.([]interface{}); ok {
		return b
	}
	panic(fmt.Sprintf("expected list got '%v'", a))
}

func mustArgs(name string, env map[string]interface{}, args []interface{}, argTypes ...string) []interface{} {
	values := []interface{}{}
	for _, a := range args {
		values = append(values, eval(env, a))
	}
	for i, at := range argTypes {
		if at == "number" {
			if _, ok := values[i].(int64); !ok {
				panic(fmt.Sprintf("%s: expected arg %d to be %s, got: %v", name, i+1, at, values[i]))
			}
		}
		if at == "list" {
			if _, ok := values[i].([]interface{}); !ok {
				panic(fmt.Sprintf("%s: expected arg %d to be %s, got: %v", name, i+1, at, values[i]))
			}
		}
	}
	return values
}

const stdlib = `
(def t (quote t))
(def nil ())

(def car (fn (a) (nth 0 a)))
(def cdr (fn (a) (slice 1 0 a)))
(def cons (fn (a b) (concat a b)))

`
