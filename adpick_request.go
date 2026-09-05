package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const adpickMaximumRequests = 320

type adpickClient struct {
	key, baseURL      string
	client            *http.Client
	interval          time.Duration
	lastSearch        time.Time // Last attempt on either endpoint; one quota across the key.
	mu                sync.Mutex
	now               func() time.Time
	sleep             func(context.Context, time.Duration) error
	requests, retries int
	coverage          *adpickCoverage
	progress          func() error
	directoryOnly     bool
}

func newAdpickClient(key string) (*adpickClient, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, fmt.Errorf("ADPICK_BIZ_API_KEY is required")
	}
	if len(key) > 512 || strings.ContainsAny(key, "/?#\\\r\n\t ") {
		return nil, fmt.Errorf("invalid ADPICK_BIZ_API_KEY")
	}
	return &adpickClient{key: key, baseURL: adpickAPIBase, interval: adpickSearchInterval,
		now: time.Now, sleep: adpickSleep,
		client: &http.Client{Timeout: 25 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func adpickSleep(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func adpickRetryDelay(raw string, now time.Time, attempt int) (time.Duration, error) {
	delay := time.Duration(attempt+1) * 10 * time.Second
	if raw == "" {
		return delay, nil
	}
	var requested time.Duration
	if seconds, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil {
		if seconds < 0 || seconds > 300 {
			return 0, fmt.Errorf("Adpick retry delay exceeds bounded run")
		}
		requested = time.Duration(seconds) * time.Second
	} else if when, err := http.ParseTime(raw); err == nil {
		requested = when.Sub(now)
	} else {
		return 0, fmt.Errorf("Adpick retry delay rejected")
	}
	if requested > 5*time.Minute {
		return 0, fmt.Errorf("Adpick retry delay exceeds bounded run")
	}
	if requested > delay {
		delay = requested
	}
	return delay, nil
}

func (c *adpickClient) request(ctx context.Context, endpoint string, params url.Values, target any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if endpoint != "malls" && endpoint != "search" {
		return fmt.Errorf("unsupported Adpick read endpoint")
	}
	for attempt := 0; attempt < 3; attempt++ {
		if c.requests >= adpickMaximumRequests {
			return fmt.Errorf("Adpick request budget exhausted")
		}
		delay := time.Duration(0)
		if !c.lastSearch.IsZero() {
			delay = c.lastSearch.Add(c.interval).Sub(c.now())
		}
		if err := c.sleep(ctx, delay); err != nil {
			return err
		}
		requestURL := c.baseURL + url.PathEscape(c.key) + "/" + endpoint + "?" + params.Encode()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
		if err != nil {
			return fmt.Errorf("Adpick request configuration rejected")
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "Statground-Data-Shopping/1.0")
		c.lastSearch = c.now()
		c.requests++
		resp, transportErr := c.client.Do(req)
		// The API credential is part of the URL. Never return transport errors,
		// redirect locations, response bodies or provider error text.
		if transportErr != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
			if attempt == 2 {
				return fmt.Errorf("Adpick %s transport failure", endpoint)
			}
			c.retries++
			if err := c.sleep(ctx, time.Duration(attempt+1)*10*time.Second); err != nil {
				return err
			}
			continue
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			retryable := resp.StatusCode == 429 || resp.StatusCode >= 500 || (resp.StatusCode == 403 && resp.Header.Get("Retry-After") != "")
			if !retryable || attempt == 2 {
				return fmt.Errorf("Adpick %s HTTP status %d", endpoint, resp.StatusCode)
			}
			delay, err := adpickRetryDelay(resp.Header.Get("Retry-After"), c.now(), attempt)
			if err != nil {
				return err
			}
			c.retries++
			if err := c.sleep(ctx, delay); err != nil {
				return err
			}
			continue
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, adpickResponseLimit+1))
		_ = resp.Body.Close()
		if err != nil || len(body) > adpickResponseLimit {
			return fmt.Errorf("Adpick %s response unavailable or oversized", endpoint)
		}
		var envelope struct {
			Success bool            `json:"success"`
			Data    json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil || !envelope.Success || len(envelope.Data) == 0 || envelope.Data[0] != '[' {
			return fmt.Errorf("Adpick %s unsuccessful response", endpoint)
		}
		if err := json.Unmarshal(envelope.Data, target); err != nil {
			return fmt.Errorf("Adpick %s invalid data", endpoint)
		}
		return nil
	}
	return fmt.Errorf("Adpick retry budget exhausted")
}
