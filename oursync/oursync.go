package oursync

import "sync/atomic"

type Int32 struct{ v int32 }
type Int64 struct{ v int64 }
type Uint32 struct{ v uint32 }
type Uint64 struct{ v uint64 }

func (a *Int32) Add(delta int32) int32    { return atomic.AddInt32(&a.v, delta) }
func (a *Int64) Add(delta int64) int64    { return atomic.AddInt64(&a.v, delta) }
func (a *Uint32) Add(delta uint32) uint32 { return atomic.AddUint32(&a.v, delta) }
func (a *Uint64) Add(delta uint64) uint64 { return atomic.AddUint64(&a.v, delta) }

func (a *Int32) Load() int32   { return atomic.LoadInt32(&a.v) }
func (a *Int64) Load() int64   { return atomic.LoadInt64(&a.v) }
func (a *Uint32) Load() uint32 { return atomic.LoadUint32(&a.v) }
func (a *Uint64) Load() uint64 { return atomic.LoadUint64(&a.v) }

func (a *Int32) Store(v int32)   { atomic.StoreInt32(&a.v, v) }
func (a *Int64) Store(v int64)   { atomic.StoreInt64(&a.v, v) }
func (a *Uint32) Store(v uint32) { atomic.StoreUint32(&a.v, v) }
func (a *Uint64) Store(v uint64) { atomic.StoreUint64(&a.v, v) }

func (a *Int32) Swap(v int32) int32    { return atomic.SwapInt32(&a.v, v) }
func (a *Int64) Swap(v int64) int64    { return atomic.SwapInt64(&a.v, v) }
func (a *Uint32) Swap(v uint32) uint32 { return atomic.SwapUint32(&a.v, v) }
func (a *Uint64) Swap(v uint64) uint64 { return atomic.SwapUint64(&a.v, v) }
