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
