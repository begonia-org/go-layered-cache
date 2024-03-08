package gobloom

import (
	"testing"

	c "github.com/smartystreets/goconvey/convey"
)

func TestBloom(t *testing.T) {
	c.Convey("test local bloom", t, func() {
		bl := NewGoBloom(1000, 0.01, BLOOM_OPT_NO_SCALING|BLOOM_OPT_NOROUND)
		c.So(bl.Add([]byte("item2")), c.ShouldBeTrue)
		c.So(bl.Test([]byte("item2")), c.ShouldBeTrue)
		// t.Log(7567644248328169922%11072)
	})
}
