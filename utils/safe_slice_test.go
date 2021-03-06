package utils

import "testing"

func TestContails(t *testing.T) {
	list := NewSafeSlice()
	list.Apppend("1")
	list.Apppend("2")
	list.Apppend("3")
	list.Remove("1")
	list.Apppend("4")
	if list.Contains("test") {
		t.Error("fail")
	}
	res := list.Get(0)
	res2 := list.Get(1)
	t.Log(list.Size())
	t.Logf("0 is:%s,1 is :%s 2 is:%s", res, res2, list.Get(2))
}
