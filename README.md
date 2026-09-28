# Batcher

Batcher is a go library that allows to collect any items into a buffer and
flush the buffer either when the buffer fills up or when timeout is reached.

The consumer must read `C()` until it is closed. The output channel holds
only 10 batches, so if nobody reads it, `Add` and `Close` block.

# Installation
```bash
go get github.com/pushwoosh/batcher
```

# Example
```go
func main() {
	b := batcher.New[int](time.Millisecond*5, 20000)

	go func() {
		for i := 0; i < 1234567; i++ {
			b.Add(i)
		}
		b.Close()
	}()

	for batch := range b.C() {
		fmt.Println("batch size:", len(batch))
	}
}
```
