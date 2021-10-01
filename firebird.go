package firebird

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/nakagami/firebirdsql"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

type Config struct {
	DriverName        string
	DSN               string
	DefaultStringSize int
	Conn              gorm.ConnPool
}

type Dialector struct {
	*Config
}

func (dialector Dialector) Name() string {
	return "firebird"
}

func Open(dsn string) gorm.Dialector {
	return &Dialector{Config: &Config{DSN: dsn}}
}

func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

func (dialector Dialector) Initialize(db *gorm.DB) (err error) {

	// register callbacks
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{})
	if dialector.DriverName == "" {
		dialector.DriverName = "firebirdsql"
	}

	if dialector.Conn != nil {
		db.ConnPool = dialector.Conn
	} else {
		db.ConnPool, err = sql.Open(dialector.DriverName, dialector.DSN)
		if err != nil {
			return err
		}
	}

	for k, v := range dialector.ClauseBuilders() {
		db.ClauseBuilders[k] = v
	}
	return
}

func (dialector Dialector) ClauseBuilders() map[string]clause.ClauseBuilder {
	return map[string]clause.ClauseBuilder{
		"LIMIT": func(c clause.Clause, builder clause.Builder) {
			if limit, ok := c.Expression.(clause.Limit); ok {
				builder.WriteString("ROWS ")
				builder.WriteString(strconv.Itoa(limit.Limit))
				if limit.Offset > 0 {
					builder.WriteString(" TO ")
					builder.WriteString(strconv.Itoa(limit.Offset))
				}
			}
		},
	}
}

func (dialector Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "NULL"}
}

func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('"')
	if strings.Contains(str, ".") {
		for idx, str := range strings.Split(str, ".") {
			if idx > 0 {
				writer.WriteString(`."`)
			}
			writer.WriteString(str)
			writer.WriteByte('"')
		}
	} else {
		writer.WriteString(str)
		writer.WriteByte('"')
	}
}

func (dialector Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{
		Migrator: migrator.Migrator{
			Config: migrator.Config{
				DB:                          db,
				Dialector:                   dialector,
				CreateIndexAfterCreateTable: true,
			},
		},
	}
}

func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	// writer.WriteString("@p")
	// writer.WriteString(strconv.Itoa(len(stmt.Vars)))
	writer.WriteByte('?')
}

// func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
// 	writer.WriteByte('"')
// 	if strings.Contains(str, ".") {
// 		for idx, str := range strings.Split(str, ".") {
// 			if idx > 0 {
// 				writer.WriteString(`."`)
// 			}
// 			writer.WriteString(str)
// 			writer.WriteByte('"')
// 		}
// 	} else {
// 		writer.WriteString(str)
// 		writer.WriteByte('"')
// 	}
// }

// var numericPlaceholder = regexp.MustCompile("@p(\\d+)")

func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}

func (dialector Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "char"
	case schema.Int, schema.Uint:
		var sqlType string
		switch {
		case field.Size < 16:
			sqlType = "smallint"
		case field.Size < 31:
			sqlType = "integer"
		default:
			sqlType = "bigint"
		}

		if field.AutoIncrement {
			return sqlType + " IDENTITY(1,1)"
		}
		return sqlType
	case schema.Float:
		return "decimal"
	case schema.String:
		size := field.Size
		hasIndex := field.TagSettings["INDEX"] != "" || field.TagSettings["UNIQUE"] != ""
		if (field.PrimaryKey || hasIndex) && size == 0 {
			if dialector.DefaultStringSize > 0 {
				size = dialector.DefaultStringSize
			} else {
				size = 256
			}
		}
		if size > 0 && size <= 4000 {
			return fmt.Sprintf("varchar(%d)", size)
		}
		return "varchar(MAX)"
	case schema.Time:
		return "datetime"
	case schema.Bytes:
		return "varbinary(MAX)"
	default:
		return "varchar"
	}

	return string(field.DataType)
}

// func (dialector Dialector) SavePoint(tx *gorm.DB, name string) error {
// 	tx.Exec("SAVEPOINT " + name)
// 	return nil
// }

// func (dialector Dialector) RollbackTo(tx *gorm.DB, name string) error {
// 	tx.Exec("ROLLBACK TO SAVEPOINT " + name)
// 	return nil
// }
