package migration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/elastic/go-elasticsearch/v7/esapi"
)

func decodeResponse(r io.ReadCloser, i interface{}) error {
	return json.NewDecoder(r).Decode(i)
}

func encodeRequest(body interface{}) io.Reader {
	b, err := json.Marshal(body)
	if err != nil {
		// we should know that `body` is a serializable struct before invoking `encodeRequest`
		panic(err)
	}

	return bytes.NewReader(b)
}

func getErrorFromESResponse(res *esapi.Response, err error) error {
	if err != nil {
		return err
	}

	if res.IsError() {
		return fmt.Errorf("response error from ES: %d", res.StatusCode)
	}
	return nil
}
