package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// apiClient is a small hand-rolled REST client for the CLI. Aegis's own Go
// SDK (sdk/go) targets worker processes talking the WebSocket dispatch
// protocol; the CLI only ever needs simple request/response JSON calls, so
// it gets its own minimal client rather than pulling in the SDK.
type apiClient struct {
	baseURL string
	http    *http.Client
}

func newClient(baseURL string) *apiClient {
	return &apiClient{baseURL: baseURL, http: &http.Client{Timeout: 15 * time.Second}}
}

func (c *apiClient) do(method, path string, query url.Values, body any, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}
	u := c.baseURL + path
	if query != nil && len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequest(method, u, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("connecting to aegis server at %s: %w", c.baseURL, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		var apiErr struct {
			Error string `json:"error"`
		}
		json.Unmarshal(data, &apiErr)
		if apiErr.Error != "" {
			return fmt.Errorf("server error (%d): %s", resp.StatusCode, apiErr.Error)
		}
		return fmt.Errorf("server error (%d): %s", resp.StatusCode, string(data))
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}
