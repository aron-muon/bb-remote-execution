package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type brokerClient struct {
	baseURL    string
	httpClient *http.Client
}

type brokerTokenRequest struct {
	Nonce       string `json:"nonce"`
	Destination string `json:"destination"`
}

type brokerTokenResponse struct {
	Token     string `json:"token"`
	ExpiresAt string `json:"expires_at,omitempty"`
	Scheme    string `json:"scheme"`
}

func newBrokerClient(baseURL string) *brokerClient {
	return &brokerClient{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 10 * 1e9}, // 10s
	}
}

func (c *brokerClient) fetchToken(ctx context.Context, nonce, destination string) (string, error) {
	body, err := json.Marshal(brokerTokenRequest{Nonce: nonce, Destination: destination})
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/token", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("POST /token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return "", fmt.Errorf("POST /token for %q: HTTP %d: %s", destination, resp.StatusCode, snippet)
	}

	var tokenResp brokerTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", fmt.Errorf("decode /token response: %w", err)
	}
	return tokenResp.Token, nil
}
