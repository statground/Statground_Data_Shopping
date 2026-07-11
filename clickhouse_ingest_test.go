package main

import (
	"errors"
	"testing"
)

type recordingRowPublisher struct {
	batches [][]Row
	fail    bool
}

func (p *recordingRowPublisher) Publish(rows []Row) error {
	if p.fail {
		return errors.New("temporary publish failure")
	}
	p.batches = append(p.batches, append([]Row(nil), rows...))
	return nil
}

func TestBufferedRowPublisherFlushesConfiguredBatch(t *testing.T) {
	recorder := &recordingRowPublisher{}
	batcher := NewBufferedRowPublisher(recorder, 2)
	if err := batcher.Add(Row{"상품코드": "1"}); err != nil {
		t.Fatal(err)
	}
	if len(recorder.batches) != 0 {
		t.Fatalf("unexpected early publish: %d batches", len(recorder.batches))
	}
	if err := batcher.Add(Row{"상품코드": "2"}); err != nil {
		t.Fatal(err)
	}
	if len(recorder.batches) != 1 || len(recorder.batches[0]) != 2 {
		t.Fatalf("published batches = %#v", recorder.batches)
	}
}

func TestBufferedRowPublisherRetainsRowsAfterFailure(t *testing.T) {
	recorder := &recordingRowPublisher{fail: true}
	batcher := NewBufferedRowPublisher(recorder, 2)
	_ = batcher.Add(Row{"상품코드": "1"})
	_ = batcher.Add(Row{"상품코드": "2"})
	_ = batcher.Add(Row{"상품코드": "3"})
	if err := batcher.Flush(); err == nil {
		t.Fatal("expected flush failure")
	}
	if got := len(batcher.PendingRows()); got != 3 {
		t.Fatalf("pending rows = %d, want 3", got)
	}
	recorder.fail = false
	if err := batcher.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := len(batcher.PendingRows()); got != 0 {
		t.Fatalf("pending rows after recovery = %d", got)
	}
}
