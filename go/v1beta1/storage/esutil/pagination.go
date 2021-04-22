// Copyright 2021 The Rode Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package esutil

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const pageTokenSeparator = ":"

func ParsePageToken(pageToken string) (string, int, error) {
	parts := strings.Split(pageToken, pageTokenSeparator)

	if len(parts) != 2 {
		return "", 0, errors.New(fmt.Sprintf("error parsing page token, expected two parts split by %s, got %d", pageTokenSeparator, len(parts)))
	}

	from, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, err
	}

	return parts[0], from, nil
}

func CreatePageToken(pit string, from int) string {
	return fmt.Sprintf("%s:%s", pit, strconv.Itoa(from))
}
