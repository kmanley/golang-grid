package grid

import (
	_ "fmt"
	"runtime"
	"testing"
)

func assertTrue(t *testing.T, cond bool, fmt string, items ...interface{}) {
	if !cond {
		if fmt == "" {
			fmt = "expected True, got False"
		}
		buf := make([]byte, 32768)
		runtime.Stack(buf, false)
		fmt = fmt + "\n" + string(buf)
		t.Errorf(fmt, items...)
	}
}
