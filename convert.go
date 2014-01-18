package freetds

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"
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
)

var sqlStartTime = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)

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
		value := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
		value = value.Add(time.Duration(days) * time.Hour * 24).Add(time.Duration(sec) * time.Second / 300)
		return value
	case SYBDATETIME4:
		var days uint16 /* number of days since 1/1/1900 */
		var mins uint16 /* number of minutes since midnight */
		binary.Read(buf, binary.LittleEndian, &days)
		binary.Read(buf, binary.LittleEndian, &mins)
		value := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
		value = value.Add(time.Duration(days) * time.Hour * 24).Add(time.Duration(mins) * time.Minute)
		return value
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
		//TODO - decimal & numeric datatypes
	default: //string
		len := strings.Index(string(data), "\x00")
		if len == -1 {
			return string(data)
		}
		return string(data[:len])
	}
}

func typeToSqlBuf(datatype int, value interface{}) (data []byte, err error) {
	buf := new(bytes.Buffer)
	switch datatype {
	case SYBINT1:
		{
			if typedValue, ok := value.(uint8); ok {
				err = binary.Write(buf, binary.LittleEndian, typedValue)
			} else {
				err = errors.New(fmt.Sprintf("Could not convert %T to uint8.", value))
			}
		}
	case SYBINT2:
		{
			if typedValue, ok := value.(int16); ok {
				err = binary.Write(buf, binary.LittleEndian, typedValue)
			} else {
				err = errors.New(fmt.Sprintf("Could not convert %T to int16.", value))
			}
		}
	case SYBINT4:
		{
			var int32Value int32
			switch value.(type) {
			case int:
				{
					intValue, _ := value.(int)
					int32Value = int32(intValue)
				}
			case int32:
				int32Value, _ = value.(int32)
			case int64:
				intValue, _ := value.(int64)
				int32Value = int32(intValue)
			default:
				{
					err = errors.New(fmt.Sprintf("Could not convert %T to int32.", value))
					return
				}
			}
			err = binary.Write(buf, binary.LittleEndian, int32Value)
		}
	case SYBINT8:
		{
			if typedValue, ok := value.(int64); ok {
				err = binary.Write(buf, binary.LittleEndian, typedValue)
			} else {
				err = errors.New(fmt.Sprintf("Could not convert %T to int64.", value))
			}
		}
	case SYBREAL:
		{
			if typedValue, ok := value.(float32); ok {
				err = binary.Write(buf, binary.LittleEndian, typedValue)
			} else {
				err = errors.New(fmt.Sprintf("Could not convert %T to float32.", value))
			}
		}
	case SYBFLT8:
		{
			if typedValue, ok := value.(float64); ok {
				err = binary.Write(buf, binary.LittleEndian, typedValue)
			} else {
				err = errors.New(fmt.Sprintf("Could not convert %T to float64.", value))
			}
		}
	case SYBBIT, SYBBITN:
		if typedValue, ok := value.(bool); ok {
			if typedValue {
				data = []byte{1}
			} else {
				data = []byte{0}
			}
			return
		} else {
			err = errors.New(fmt.Sprintf("Could not convert %T to bool.", value))
		}
	case SYBMONEY4:
		{
			if typedValue, ok := value.(float64); ok {
				intValue := int32(typedValue * 10000)
				err = binary.Write(buf, binary.LittleEndian, intValue)
			} else {
				err = errors.New(fmt.Sprintf("Could not convert %T to float64.", value))
			}
		}
	case SYBMONEY:
		{
			if typedValue, ok := value.(float64); ok {
				intValue := int64(typedValue * 10000)
				high := int32(intValue >> 32)
				low := uint32(intValue - int64(high))
				err = binary.Write(buf, binary.LittleEndian, high)
				if err == nil {
					err = binary.Write(buf, binary.LittleEndian, low)
				}
			} else {
				err = errors.New(fmt.Sprintf("Could not convert %T to float64.", value))
			}
		}
	case SYBDATETIME:
		{
			if typedValue, ok := value.(time.Time); ok {
				typedValue = typedValue.UTC()
				days := int32(typedValue.Sub(sqlStartTime).Hours() / 24)
				secs := uint32((((typedValue.Hour()*60+typedValue.Minute())*60)+typedValue.Second())*300 +
					typedValue.Nanosecond()/3333333)
				err = binary.Write(buf, binary.LittleEndian, days)
				if err == nil {
					err = binary.Write(buf, binary.LittleEndian, secs)
				}
			} else {
				err = errors.New(fmt.Sprintf("Could not convert %T to time.Time.", value))
			}
		}
	case SYBDATETIME4:
		{
			if typedValue, ok := value.(time.Time); ok {
				typedValue = typedValue.UTC()
				days := uint16(typedValue.Sub(sqlStartTime).Hours() / 24)
				mins := uint16(typedValue.Hour()*60 + typedValue.Minute())
				err = binary.Write(buf, binary.LittleEndian, days)
				if err == nil {
					err = binary.Write(buf, binary.LittleEndian, mins)
				}
			} else {
				err = errors.New(fmt.Sprintf("Could not convert %T to time.Time.", value))
			}
		}
	case SYBIMAGE, SYBVARBINARY, SYBBINARY, XSYBVARBINARY:
		if typedValue, ok := value.([]byte); ok {
			data = append(typedValue, []byte{0}[0])
			return
		} else {
			err = errors.New(fmt.Sprintf("Could not convert %T to []byte.", value))
		}
	default:
		{
			if typedValue, ok := value.(string); ok {
				data = []byte(typedValue)
				if datatype == XSYBNVARCHAR ||
					datatype == XSYBNCHAR {
					//FIXME - adding len bytes to the end of the buf
					//        realy don't understand why this is necessary
					//        come to this solution by try and error
					l := len(data)
					for i := 0; i < l; i++ {
						data = append(data, byte(0))
					}
				}
				return
			} else {
				err = errors.New(fmt.Sprintf("Could not convert %T to string.", value))
			}
		}
	}
	data = buf.Bytes()
	return
}
