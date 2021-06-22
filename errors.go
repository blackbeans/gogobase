/*


 */

package goh

import (
	"bytes"
	"git.uneed.com/server/unicom/hadoop/goh/proto"
)

/*
HbaseError
*/
type HbaseError struct {
	IOErr  *proto.IOError         // IOError
	ArgErr *proto.IllegalArgument // IllegalArgument
	Err    error                  // error

}

func newHbaseError(arg *proto.IllegalArgument, err error) *HbaseError {
	return &HbaseError{
		ArgErr: arg,
		Err:    err,
	}
}

/*
String
*/
func (e *HbaseError) String() string {
	if e == nil {
		return "<nil>"
	}

	var b bytes.Buffer
	if e.IOErr != nil {
		b.WriteString("IOError:")
		b.WriteString(e.IOErr.Message)
		b.WriteString(";")
	}

	if e.ArgErr != nil {
		b.WriteString("ArgumentError:")
		b.WriteString(e.ArgErr.Message)
		b.WriteString(";")
	}

	if e.Err != nil {
		b.WriteString("Error:")
		b.WriteString(e.Err.Error())
		b.WriteString(";")
	}
	return b.String()
}

/*
Error
*/
func (e *HbaseError) Error() string {
	return e.String()
}

func checkHbaseError(err error) error {
	if err != nil {
		return newHbaseError(nil, err)
	}
	return nil
}
