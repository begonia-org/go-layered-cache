// Copyright 2024 geebytes. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package golayeredcache

import (
	"github.com/begonia-org/go-layered-cache/local"
)

type LayeredFilterOptions func(layered LayeredFilter) error
type WithLayeredCuckooOptions func(layered LayeredCuckooFilter) error

func WithInitLocalFilterOptions(filters map[string]local.Filter) LayeredFilterOptions {
	return func(layered LayeredFilter) error {
		for k, v := range filters {
			err := layered.AddLocalFilter(k, v)
			if err != nil {
				return err
			}
		}
		return nil
	}
}
