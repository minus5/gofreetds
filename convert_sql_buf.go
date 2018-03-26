package freetds

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	//name               database type   go type
	SYBINT1 = 48  //tinyint       uint8
	SYBINT2 = 52  //smallint      int16
	SYBINT4 = 56  //int           int32
	SYBINT8 = 127 //bigint        int64

	SYBCHAR      = 47
	SYBVARCHAR   = 39  //varchar       string
	SYBNVARCHAR  = 103 //nvarchar      string
	XSYBNVARCHAR = 231 //nvarchar      string
	XSYBNCHAR    = 239 //nchar         string
	XSYBXML      = 241 //XML           string

	SYBREAL = 59 //real          float32
	SYBFLT8 = 62 //float(53)     float64

	SYBBIT  = 50  //bit           bool
	SYBBITN = 104 //bit           bool

	SYBMONEY4 = 122 //smallmoney    float64
	SYBMONEY  = 60  //money         float64

	SYBDATETIME  = 61 //datetime      time.Time
	SYBDATETIME4 = 58 //smalldatetime time.Time

	SYBIMAGE      = 34  //image         []byte
	SYBBINARY     = 45  //binary        []byte
	SYBVARBINARY  = 37  //varbinary     []byte
	XSYBVARBINARY = 165 //varbinary     []byte

	SYBNUMERIC = 108
	SYBDECIMAL = 106

	SYBUNIQUE = 36 //uniqueidentifier string
)

var (
	sqlStartTime          = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	sqlMaxTime            = time.Date(9999, 12, 31, 23, 59, 59, 997, time.UTC)
	sqlMaxTimeDays int32  = 2958463
	sqlMaxTimeSec  uint32 = 25919999
	sqlMinTime            = time.Date(1753, 01, 01, 00, 00, 00, 000, time.UTC)
	sqlMinTimeDays int32  = -53690
	sqlMinTimeSec  uint32 = 0
)

func toLocalTime(value time.Time) time.Time {
	value = value.In(time.Local)
	value0 := value
	// remove timezone offset
	_, of := value.Zone()
	value = value.Add(time.Duration(-of) * time.Second)
	// compensate day light saving failures
	if _, of2 := value.Zone(); of2 != of {
		value = value0.Add(time.Duration(-of2) * time.Second)
	}
	return value
}

func sqlBufToType(datatype int, data []byte) interface{} {
	buf := bytes.NewBuffer(data)
	switch datatype {
	case SYBINT1:
		var value uint8
		binary.Read(buf, binary.LittleEndian, &value)
		return value
	case SYBINT2:
		var value int16
		binary.Read(buf, binary.LittleEndian, &value)
		return value
	case SYBINT4:
		var value int32
		binary.Read(buf, binary.LittleEndian, &value)
		return value
	case SYBINT8:
		var value int64
		binary.Read(buf, binary.LittleEndian, &value)
		return value
	case SYBDATETIME:
		var days int32 /* number of days since 1/1/1900 */
		var sec uint32 /* 300ths of a second since midnight */
		binary.Read(buf, binary.LittleEndian, &days)
		binary.Read(buf, binary.LittleEndian, &sec)
		if days == sqlMaxTimeDays && sec == sqlMaxTimeSec {
			// Do not modify the time using an offset, just change the timezone.
			return sqlMaxTime.Local()
		} else if days == sqlMinTimeDays && sec == sqlMinTimeSec {
			// Do not modify the time using an offset, just change the timezone.
			return sqlMinTime.Local()
		} else {
			value := sqlStartTime.Add(time.Duration(days) * time.Hour * 24).Add(time.Duration(sec) * time.Second / 300)
			return toLocalTime(value)
		}
	case SYBDATETIME4:
		var days uint16 /* number of days since 1/1/1900 */
		var mins uint16 /* number of minutes since midnight */
		binary.Read(buf, binary.LittleEndian, &days)
		binary.Read(buf, binary.LittleEndian, &mins)
		value := sqlStartTime.Add(time.Duration(days) * time.Hour * 24).Add(time.Duration(mins) * time.Minute)
		return toLocalTime(value)
	case SYBMONEY:
		var high int32
		var low uint32
		binary.Read(buf, binary.LittleEndian, &high)
		binary.Read(buf, binary.LittleEndian, &low)
		return float64(int64(high)*4294967296+int64(low)) / 10000
	case SYBMONEY4:
		var value int32
		binary.Read(buf, binary.LittleEndian, &value)
		return float64(value) / 10000
	case SYBREAL:
		var value float32
		binary.Read(buf, binary.LittleEndian, &value)
		return value
	case SYBFLT8:
		var value float64
		binary.Read(buf, binary.LittleEndian, &value)
		return value
	case SYBBIT, SYBBITN:
		return data[0] == 1
	case SYBIMAGE, SYBVARBINARY, SYBBINARY, XSYBVARBINARY:
		return append([]byte{}, data[:len(data)-1]...) // make copy of data

	default: //string
		len := strings.Index(string(data), "\x00")
		if len == -1 {
			return string(data)
		}
		return string(data[:len])
	}
}

func typeToSqlBuf(datatype int, value interface{}, freetdsVersionGte095 bool) (data []byte, datalen int, err error) {
	datalen = -1
	buf := new(bytes.Buffer)
	switch datatype {
	case SYBINT1:
		var ui8 uint8
		if err = convertAssign(&ui8, value); err == nil {
			err = binary.Write(buf, binary.LittleEndian, ui8)
		}
	case SYBINT2:
		var i16 int16
		if err = convertAssign(&i16, value); err == nil {
			err = binary.Write(buf, binary.LittleEndian, i16)
		}
	case SYBINT4:
		var i32 int32
		if err = convertAssign(&i32, value); err == nil {
			err = binary.Write(buf, binary.LittleEndian, i32)
		}
	case SYBINT8:
		var i64 int64
		if err = convertAssign(&i64, value); err == nil {
			err = binary.Write(buf, binary.LittleEndian, i64)
		}
	case SYBREAL:
		var f32 float32
		if err = convertAssign(&f32, value); err == nil {
			err = binary.Write(buf, binary.LittleEndian, f32)
		}
	case SYBFLT8:
		var f64 float64
		if err = convertAssign(&f64, value); err == nil {
			err = binary.Write(buf, binary.LittleEndian, f64)
		}
	case SYBBIT, SYBBITN:
		if typedValue, ok := value.(bool); ok {
			if typedValue {
				data = []byte{1}
			} else {
				data = []byte{0}
			}
			datalen = 1
			return
		} else {
			err = errors.New(fmt.Sprintf("Could not convert %T to bool.", value))
		}
	case SYBMONEY4:
		var f64 float64
		if err = convertAssign(&f64, value); err == nil {
			intValue := int64((f64 + 0.000000000000001) * 10000)
			i32 := int32(intValue)
			err = binary.Write(buf, binary.LittleEndian, i32)
		}
	case SYBMONEY:
		var f64 float64
		if err = convertAssign(&f64, value); err == nil {
			intValue := int64((f64 + 0.000000000000001) * 10000)
			high := int32(intValue >> 32)
			low := uint32(intValue - int64(high))
			err = binary.Write(buf, binary.LittleEndian, high)
			if err == nil {
				err = binary.Write(buf, binary.LittleEndian, low)
			}
		}
	case SYBDATETIME:
		//database time is always in local timezone
		if tm, ok := value.(time.Time); ok {
			var days int32
			var secs uint32
			// Skip the math and just use constants for SQL Max or Min Time
			if tm.Equal(sqlMaxTime) {
				days = sqlMaxTimeDays
				secs = sqlMaxTimeSec
			} else if tm.IsZero() {
				// Go zero time => mssql null
				return
			} else if tm.Equal(sqlMinTime) {
				days = sqlMinTimeDays
				secs = sqlMinTimeSec
			} else {
				tm = tm.Local()
				diff := tm.UnixNano() - sqlStartTime.UnixNano()
				_, of := tm.Zone()
				diff += int64(time.Duration(of) * time.Second)
				days = int32(diff / 1e9 / 60 / 60 / 24)
				secs = uint32(float64(diff-int64(days)*1e9*60*60*24) * 0.0000003)
			}
			err = binary.Write(buf, binary.LittleEndian, days)
			if err == nil {
				err = binary.Write(buf, binary.LittleEndian, secs)
			}
		} else {
			err = errors.New(fmt.Sprintf("Could not convert %T to time.Time.", value))
		}
	case SYBDATETIME4:
		if tm, ok := value.(time.Time); ok {
			tm = tm.Local()
			diff := tm.Unix() - sqlStartTime.Unix()
			_, of := tm.Zone()
			diff += int64(of)
			days := uint16(diff / 60 / 60 / 24)
			mins := uint16((diff - int64(days)*60*60*24) / 60)
			err = binary.Write(buf, binary.LittleEndian, days)
			if err == nil {
				err = binary.Write(buf, binary.LittleEndian, mins)
			}
		} else {
			err = errors.New(fmt.Sprintf("Could not convert %T to time.Time.", value))
		}
	case SYBIMAGE, SYBVARBINARY, SYBBINARY, XSYBVARBINARY:
		if buf, ok := value.([]byte); ok {
			data = append(buf, []byte{0}[0])
			datalen = len(data)
			return
		} else {
			err = errors.New(fmt.Sprintf("Could not convert %T to []byte.", value))
		}
	default:
		if str, ok := value.(string); ok {
			if str == "" {
				//dbrpcparam treats any data with datalen 0 as NULL value
				//(rpc.c line 241 in freetds)
				//It is kinda safe to put this into db if len function is used to check for emtpy strings.
				//Len strips trailing spaces, and returns 0 for ' '.
				//Links:
				//  https://github.com/pymssql/pymssql/issues/243
				//  http://stackoverflow.com/questions/2025585/len-function-not-including-trailing-spaces-in-sql-server
				data = []byte{32}
				datalen = 1
				return
			}
			data = []byte(str)
			datalen = len(data)
			if !freetdsVersionGte095 {
				if datatype == XSYBNVARCHAR || datatype == XSYBNCHAR {
					//FIXME - adding len bytes to the end of the buf
					//        realy don't understand why this is necessary
					//        come to this solution by try and error
					l := len(data)
					for i := 0; i < l; i++ {
						data = append(data, byte(0))
					}
				}
				datalen = len(data)
			} else {
				//set datalen for nvarchar and nchar datatypes
				if datatype == XSYBNCHAR {
					runelen := utf8.RuneCountInString(str)
					if runelen*2 > len(data) {
						datalen = runelen * 2
					}
				}
			}
			//fmt.Printf("data %s\n", str)
			//fmt.Printf("datalen %d %d %v %d %v %d\n", datalen, len(data), freetdsVersionGte095, datatype, datatype == XSYBNVARCHAR && !freetdsVersionGte095, utf8.RuneCountInString(str))
			return
		} else {
			err = errors.New(fmt.Sprintf("Could not convert %T to string.", value))
		}
	}
	data = buf.Bytes()
	datalen = len(data)
	return
}
