package delayq

import (
	"fmt"
	"time"
)

type OptionType int

const (
	ProcessAtOpt OptionType = iota
	ProcessInOpt
	TimeoutOpt
	RetryOpt
)

type (
	timeoutOption   time.Duration
	processAtOption time.Time
	processInOption time.Duration
	retryOption     int64
)

func Retry(d int64) Option {
	return retryOption(d)
}
func (r retryOption) String() string {
	return fmt.Sprintf("Retry(%v)", int64(r))
}

func (r retryOption) Type() OptionType {
	return RetryOpt
}

func (r retryOption) Value() interface{} {
	return r
}

func Timeout(d time.Duration) Option {
	return timeoutOption(d)
}
func (d timeoutOption) String() string     { return fmt.Sprintf("Timeout(%v)", time.Duration(d)) }
func (d timeoutOption) Type() OptionType   { return TimeoutOpt }
func (d timeoutOption) Value() interface{} { return time.Duration(d) }

func ProcessIn(d time.Duration) Option {
	return processInOption(d)
}
func (p processAtOption) String() string {
	return fmt.Sprintf("ProcessAt(%v)", time.Time(p).Format(time.UnixDate))
}
func (p processAtOption) Type() OptionType   { return ProcessAtOpt }
func (p processAtOption) Value() interface{} { return time.Time(p) }

func ProcessAt(d time.Time) Option {
	return processAtOption(d)
}
func (p processInOption) String() string     { return fmt.Sprintf("ProcessIn(%v)", time.Duration(p)) }
func (p processInOption) Type() OptionType   { return ProcessInOpt }
func (p processInOption) Value() interface{} { return time.Duration(p) }

type Option interface {
	String() string
	Type() OptionType
	Value() interface{}
}
