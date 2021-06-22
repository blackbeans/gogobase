/*


 */

package gogohbase

import (
	"fmt"

	"github.com/blackbeans/gogobase/proto"
)

//type Text []byte

func textListToStr(list [][]byte) []string {
	if list == nil {
		return nil
	}

	l := len(list)
	data := make([]string, l)
	for i := 0; i < l; i++ {
		data[i] = string(list[i])
	}

	return data
}

func toHbaseTextList(list []string) [][]byte {
	if list == nil {
		return nil
	}

	l := len(list)
	data := make([][]byte, l)
	for i := 0; i < l; i++ {
		data[i] = []byte((list[i]))
	}
	return data
}

func toHbaseTextMap(source map[string]string) map[string]proto.Text {
	if source == nil {
		return nil
	}

	data := make(map[string]proto.Text, len(source))
	for k, v := range source {
		data[k] = proto.Text(v)
	}

	return data
}

//type Bytes []byte

//type ScannerID int32

/**
 * An HColumnDescriptor contains information about a column family
 * such as the number of versions, compression settings, etc. It is
 * used as input when creating a table or adding a column.
 *
 * Attributes:
 *  - Name
 *  - MaxVersions
 *  - Compression
 *  - InMemory
 *  - BloomFilterType
 *  - BloomFilterVectorSize
 *  - BloomFilterNbHashes
 *  - BlockCacheEnabled
 *  - TimeToLive
 */
type ColumnDescriptor struct {
	Name                  string "name"                  // 1
	MaxVersions           int32  "maxVersions"           // 2
	Compression           string "compression"           // 3
	InMemory              bool   "inMemory"              // 4
	BloomFilterType       string "bloomFilterType"       // 5
	BloomFilterVectorSize int32  "bloomFilterVectorSize" // 6
	BloomFilterNbHashes   int32  "bloomFilterNbHashes"   // 7
	BlockCacheEnabled     bool   "blockCacheEnabled"     // 8
	TimeToLive            int32  "timeToLive"            // 9
}

func toColumn(col *proto.ColumnDescriptor) *ColumnDescriptor {
	return &ColumnDescriptor{
		Name:                  string(col.Name),
		MaxVersions:           col.MaxVersions,
		Compression:           col.Compression,
		InMemory:              col.InMemory,
		BloomFilterType:       col.BloomFilterType,
		BloomFilterVectorSize: col.BloomFilterVectorSize,
		BloomFilterNbHashes:   col.BloomFilterNbHashes,
		BlockCacheEnabled:     col.BlockCacheEnabled,
		TimeToLive:            col.TimeToLive,
	}

}

func toColMap(cols map[string]*proto.ColumnDescriptor) map[string]*ColumnDescriptor {
	if cols == nil {
		return nil
	}
	data := make(map[string]*ColumnDescriptor, len(cols))
	for k, v := range cols {
		data[k] = toColumn(v)
	}
	return data
}

func toHbaseColumn(col *ColumnDescriptor) *proto.ColumnDescriptor {
	return &proto.ColumnDescriptor{
		Name:                  proto.Text(col.Name),
		MaxVersions:           col.MaxVersions,
		Compression:           col.Compression,
		InMemory:              col.InMemory,
		BloomFilterType:       col.BloomFilterType,
		BloomFilterVectorSize: col.BloomFilterVectorSize,
		BloomFilterNbHashes:   col.BloomFilterNbHashes,
		BlockCacheEnabled:     col.BlockCacheEnabled,
		TimeToLive:            col.TimeToLive,
	}

}

func toHbaseColList(cols []*ColumnDescriptor) []*proto.ColumnDescriptor {
	if cols == nil {
		return nil
	}

	l := len(cols)
	data := make([]*proto.ColumnDescriptor, l)
	for i := 0; i < l; i++ {
		data[i] = toHbaseColumn(cols[i])
	}
	return data
}

func NewColumnDescriptorDefault(name string) *ColumnDescriptor {
	output := &ColumnDescriptor{
		MaxVersions:           3,
		Compression:           "NONE",
		InMemory:              false,
		BloomFilterType:       "NONE",
		BloomFilterVectorSize: 0,
		BloomFilterNbHashes:   0,
		BlockCacheEnabled:     false,
		TimeToLive:            -1,
		Name:                  name,
	}

	return output
}

/**
 * A TRegionInfo contains information about an HTable region.
 *
 * Attributes:
 *  - StartKey
 *  - EndKey
 *  - Id
 *  - Name
 *  - Version
 *  - ServerName
 *  - Port
 */
type TRegionInfo struct {
	StartKey   string "startKey"   // 1
	EndKey     string "endKey"     // 2
	Id         int64  "id"         // 3
	Name       string "name"       // 4
	Version    int8   "version"    // 5
	ServerName string "serverName" // 6
	Port       int32  "port"       // 7
}

func toRegion(region *proto.TRegionInfo) *TRegionInfo {
	return &TRegionInfo{
		StartKey:   string(region.StartKey),
		EndKey:     string(region.EndKey),
		Id:         region.ID,
		Name:       string(region.Name),
		Version:    region.Version,
		ServerName: string(region.ServerName),
		Port:       region.Port,
	}

}

func toRegionList(regions []*proto.TRegionInfo) []*TRegionInfo {

	if regions == nil {
		return nil
	}

	l := len(regions)
	data := make([]*TRegionInfo, l)
	for i := 0; i < l; i++ {
		data[i] = toRegion(regions[i])
	}

	return data
}

// /**
//  * A Mutation object is used to either update or delete a column-value.
//  *
//  * Attributes:
//  *  - IsDelete
//  *  - Column
//  *  - Value
//  *  - WriteToWAL
//  */
// type Mutation struct {
// 	IsDelete   bool   "isDelete"   // 1
// 	Column     []byte "column"     // 2
// 	Value      []byte "value"      // 3
// 	WriteToWAL bool   "writeToWAL" // 4
// }

func NewMutation(column string, value []byte) *proto.Mutation {
	return &proto.Mutation{
		IsDelete:   false,
		WriteToWAL: true,
		Column:     proto.Text(column),
		Value:      proto.Text(value),
	}

}

// /**
//  * A BatchMutation object is used to apply a number of Mutations to a single row.
//  *
//  * Attributes:
//  *  - Row
//  *  - Mutations
//  */
// type BatchMutation struct {
// 	Row       []byte      "row"       // 1
// 	Mutations []*Mutation "mutations" // 2
// }

func NewBatchMutation(row []byte, mutations []*proto.Mutation) *proto.BatchMutation {
	return &proto.BatchMutation{
		Row:       []byte(row),
		Mutations: mutations,
	}

}

// /**
//  * For increments that are not incrementColumnValue
//  * equivalents.
//  *
//  * Attributes:
//  *  - Table
//  *  - Row
//  *  - Column
//  *  - Ammount
//  */
// type TIncrement struct {
// 	Table   []byte "table"   // 1
// 	Row     []byte "row"     // 2
// 	Column  []byte "column"  // 3
// 	Ammount int64  "ammount" // 4
// }

func NewTIncrement(table string, row []byte, column string, ammount int64) *proto.TIncrement {
	return &proto.TIncrement{
		Table:   proto.Text(table),
		Row:     proto.Text(row),
		Column:  proto.Text(column),
		Ammount: ammount,
	}
}

/**
 * Holds row name and then a map of columns to cells.
 *
 * Attributes:
 *  - Row
 *  - Columns
 */
// type TRowResult struct {
// 	Row     string            "row"     // 1
// 	Columns map[string]*TCell "columns" // 2
// }

/**
 * A Scan object is used to specify scanner parameters when opening a scanner.
 *
 * Attributes:
 *  - StartRow
 *  - StopRow
 *  - Timestamp
 *  - Columns
 *  - Caching
 *  - FilterString
 */
type TScan struct {
	StartRow     []byte   "startRow"     // 1
	StopRow      []byte   "stopRow"      // 2
	Timestamp    *int64   "timestamp"    // 3
	Columns      []string "columns"      // 4
	Caching      *int32   "caching"      // 5
	FilterString string   "filterString" // 6
	Reversed     *bool    "reversed"
	BatchSize    *int32   "batchSize"
	SortColumns  *bool    "sortColumns"
}

func toHbaseTScan(scan *TScan) *proto.TScan {
	if scan == nil {
		return nil
	}

	if scan.FilterString == "" {
		return &proto.TScan{
			StartRow:     proto.Text(scan.StartRow),
			StopRow:      proto.Text(scan.StopRow),
			Timestamp:    scan.Timestamp,
			Columns:      toHbaseTextList(scan.Columns),
			Caching:      scan.Caching,
			FilterString: nil,
			Reversed:     scan.Reversed,
			BatchSize:    scan.BatchSize,
			SortColumns:  scan.SortColumns,
		}
	}

	return &proto.TScan{
		StartRow:     proto.Text(scan.StartRow),
		StopRow:      proto.Text(scan.StopRow),
		Timestamp:    scan.Timestamp,
		Columns:      toHbaseTextList(scan.Columns),
		Caching:      scan.Caching,
		FilterString: proto.Text(scan.FilterString),
		Reversed:     scan.Reversed,
		BatchSize:    scan.BatchSize,
		SortColumns:  scan.SortColumns,
	}

}

// /**
//  * TCell - Used to transport a cell value (byte[]) and the timestamp it was
//  * stored with together as a result for get and getRow methods. This promotes
//  * the timestamp of a cell to a first-class value, making it easy to take
//  * note of temporal data. Cell is used all the way from HStore up to HTable.
//  *
//  * Attributes:
//  *  - Value
//  *  - Timestamp
//  */
// type TCell struct {
// 	Value     []byte "value"     // 1
// 	Timestamp int64  "timestamp" // 2
// }

// /**
//  * An IOError exception signals that an error occurred communicating
//  * to the Hbase master or an Hbase region server.  Also used to return
//  * more general Hbase error conditions.
//  *
//  * Attributes:
//  *  - Message
//  */
// type IOError struct {
// 	Message string "message" // 1
// }

// /**
//  * An IllegalArgument exception indicates an illegal or invalid
//  * argument was passed into a procedure.
//  *
//  * Attributes:
//  *  - Message
//  */
// type IllegalArgument struct {
// 	Message string "message" // 1
// }

// /**
//  * An AlreadyExists exceptions signals that a table with the specified
//  * name already exists
//  *
//  * Attributes:
//  *  - Message
//  */
// type AlreadyExists struct {
// 	Message string "message" // 1
// }

// func (t *AlreadyExists) String() string {
// 	if t == nil {
// 		return "<nil>"
// 	}
// 	return t.Message
// }

func name() {
	fmt.Println("...")
}
