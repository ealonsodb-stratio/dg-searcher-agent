package com.stratio.governance.agent.search.testit.utils.postgres

import com.stratio.governance.agent.search.testit.utils.postgres.PostgresSpace.Database.Schema
import com.stratio.governance.agent.search.testit.utils.postgres.PostgresSpace.Database.Schema.Table
import com.stratio.governance.agent.search.testit.utils.postgres.PostgresSpace.Database.Schema.Table.Constraint.AdditionalOption.`ON DELETE CASCADE`
import com.stratio.governance.agent.search.testit.utils.postgres.PostgresSpace.Database.Schema.Table.Constraint.Type.Token
import com.stratio.governance.agent.search.testit.utils.postgres.PostgresSpace.Database.Schema.Table._
import com.stratio.governance.agent.search.testit.utils.postgres.builder.AbstractBuilder
import com.stratio.governance.agent.search.testit.utils.postgres.connection.Connection

import scala.collection.mutable.ListBuffer

object PostgresSpace {

  object Database {
    def builder(database: String): DatabaseBuilder = DatabaseBuilder(database.toLowerCase())

    object Schema {
      def builder(name: String): SchemaBuilder = SchemaBuilder(name)

      case class Table(schema: String, _name: String, columns: List[Column], constraints: List[Constraint], inserts: List[Insert]) {
        val name: String = _name.trim.toLowerCase
        def generateCreateTableQuery: String = {
          val queryBuilder = new StringBuilder()
            .append("CREATE TABLE IF NOT EXISTS ")
            .append(s"$schema.$name (")
          var prefix = ""
          columns.foreach((column: Column) => {
            queryBuilder.append(prefix + column.generateCreateTableLine)
            prefix = ", "
          })
          constraints.foreach((constraint: Constraint) => queryBuilder.append(prefix.concat(constraint.toString)))
          queryBuilder.append(")").mkString
        }

        def create: Table = {
          Connection.defaultConnection.executeUpdate(generateCreateTableQuery)
          this
        }

        def drop: Table = {
          Connection.defaultConnection.executeUpdate(s"DROP TABLE $schema.$name CASCADE")
          this
        }

        def generateInsertQueries: List[String] = {
          inserts.map(_.generateQuery)
        }

        def insert: Table = {
          generateInsertQueries.foreach(Connection.defaultConnection.executeUpdate)
          this
        }

        def createAndInsert: Table = {
          create.insert
          this
        }

      }
      case class TableBuilder(schema: String, name: String) extends AbstractBuilder[Table] {

        val columns : ListBuffer[Column] = ListBuffer()
        val constraints : ListBuffer[Constraint] = ListBuffer()
        val inserts: ListBuffer[Insert] = ListBuffer()

        def withColumn(column: Column): TableBuilder = {
          columns += column
          this
        }

        def withColumn(column: ColumnBuilder): TableBuilder = withColumn(column.build)

        def withConstraint(constraint: Constraint): TableBuilder = {
          constraints +=  constraint
          this
        }

        def withConstraint(constraint: ConstraintBuilder): TableBuilder = withConstraint(constraint.build)

        def withInsert(insert: Insert): TableBuilder = {
          val columnsNames = columns.map(_.name)
          val insertColumnNamesNotInTableColumns = insert.values.filter((value: Table.Insert.Value) => !columnsNames.exists(value.column.name.equals(_)))
          if (insertColumnNamesNotInTableColumns.nonEmpty) {
            throw new IllegalArgumentException("You try to insert over non-created columns: ".concat(insertColumnNamesNotInTableColumns.mkString(", ")))
          }
          inserts += insert
          this
        }

        def withInsert(insert: InsertBuilder): TableBuilder = withInsert(insert.build)

        override def build: Table = Table(schema, name, columns.toList, constraints.toList, inserts.toList)

      }
      object Table {
        def builder(schema: String, name: String): TableBuilder = TableBuilder(schema, name)
        object Constraint {
          def builder(name: String, `type`: Type): ConstraintBuilder = ConstraintBuilder(name, `type`)
          def builder[T <: Type](name: String, `type`: TypeBuilder[T]): ConstraintBuilder = builder(name, `type`.build)

          object Type {
            object Token extends Enumeration {
              val UNIQUE: Token.Value = Value("UNIQUE")
              val `FOREIGN KEY`: Token.Value = Value("FOREIGN KEY")
            }
          }

          sealed class Type(token: Type.Token.Value, additionalOptions: List[AdditionalOption]) {
          }

          case class UniqueType(columns: Set[String]) extends Type(Token.UNIQUE, List()) {
            override def toString: String = {Token.UNIQUE.toString + s"(${columns.mkString(", ")})"}
          }

          object UniqueType {
            def builder(columns : Set[String]): UniqueTypeBuilder = UniqueTypeBuilder(columns)
          }

          case class ForeignKeyType(schema: String, table: String, column: String, columns : Set[String], additionalOpts: List[AdditionalOption]) extends Type(Token.`FOREIGN KEY`, additionalOpts) {
            override def toString: String = {
              s"${Token.`FOREIGN KEY`.toString} (${columns.mkString(", ")}) REFERENCES $schema.$table($column)" + this.additionalOpts.mkString(" ")
            }
          }

          object ForeignKeyType {
            def builder(schema: String, table: String, column: String, columns : Set[String]): ForeignKeyTypeBuilder = ForeignKeyTypeBuilder(schema, table, column, columns)
          }

          object AdditionalOption extends Enumeration {
            val `ON DELETE CASCADE`: PostgresSpace.Database.Schema.Table.Constraint.AdditionalOption.Value = Value("ON DELETE CASCADE")
          }

          sealed abstract class AdditionalOption(option: AdditionalOption.Value) {
            override def toString: String = option.toString
          }

          case class OnDeleteCascadeAdditionalOption() extends AdditionalOption(`ON DELETE CASCADE`)

          object OnDeleteCascadeAdditionalOption {
            def builder: OnDeleteCascadeAdditionalOptionBuilder = OnDeleteCascadeAdditionalOptionBuilder()
          }

          sealed abstract class AdditionalOptionBuilder[T <: AdditionalOption] extends AbstractBuilder[T]

          case class OnDeleteCascadeAdditionalOptionBuilder() extends AdditionalOptionBuilder[OnDeleteCascadeAdditionalOption]{
            def build: OnDeleteCascadeAdditionalOption = OnDeleteCascadeAdditionalOption()
          }

          sealed abstract class TypeBuilder[T <: Type](token: Token.Value) extends AbstractBuilder[T] {

            val additionalOptions: ListBuffer[AdditionalOption] = ListBuffer()

            def withAdditionalOption(additionalOption: AdditionalOption): TypeBuilder[T] = {
              additionalOptions += additionalOption
              this
            }

            def withAdditionalOption[K <: AdditionalOption](option: AdditionalOptionBuilder[K]): TypeBuilder[T] = withAdditionalOption(option.build)

            def build: T
          }

          case class ForeignKeyTypeBuilder(schema: String,table: String, column: String, columns : Set[String]) extends TypeBuilder[ForeignKeyType](Token.`FOREIGN KEY`) {
            override def build: ForeignKeyType = ForeignKeyType(schema, table, column, columns, additionalOptions.toList)
          }

          case class UniqueTypeBuilder(columns : Set[String]) extends TypeBuilder[UniqueType](Token.UNIQUE) {
            override def build: UniqueType = UniqueType(columns)
          }
        }

        case class Constraint(name: String, `type`: Constraint.Type) {
          override def toString: String = "CONSTRAINT ".concat(name).concat(" ").concat(`type`.toString)
        }

        case class ConstraintBuilder(name: String, `type`: Constraint.Type) extends AbstractBuilder[Constraint]{
          override def build: Constraint = Constraint(name,`type`)
        }

        case class Insert(schema: String, table: String, values: List[Insert.Value]) {

          def hasColumn(columnName: String): Boolean = values.exists((value: Insert.Value) => value.column.name.equals(columnName))


          def generateQuery: String = {
            val queryBuilder: StringBuilder = new StringBuilder().append(s"INSERT INTO $schema.$table (")
            var names: String = ""
            var values_out: String = ""
            var prefix: String = ""
            for (value <- values) {
              names += prefix + value.column.name
              values_out += prefix + value.value
              prefix = ", "

            }
            queryBuilder.append(names).append(") VALUES (").append(values).append(")").mkString
          }
        }
        object Insert {
          def builder(schema: String, table: String): InsertBuilder = InsertBuilder(schema: String, table: String)

          case class Value(column: Column, value: String)

          object Value {
            def builder(column: Column, value: String): ValueBuilder = ValueBuilder(column, value)
            def builder(column: ColumnBuilder, value: String): ValueBuilder = builder(column.build, value)
          }

          case class ValueBuilder(column: Column, value: String) extends AbstractBuilder[Value]{
            override def build: Value = Value(column, value)
          }
        }

        case class InsertBuilder(schema: String, table: String) extends AbstractBuilder[Insert] {
          val values: ListBuffer[Insert.Value] = ListBuffer()

          def withValue(value: Insert.Value): InsertBuilder = {
            values += value
            this
          }

          def withValues(inserts: List[Insert.Value]): InsertBuilder = {
            values ++= inserts
            this
          }

          def withValue(value: Insert.ValueBuilder): InsertBuilder = withValue(value.build)
          def withValues(values: Insert.ValueBuilder*): InsertBuilder = withValues(values.map(_.build).toList)

          override def build: Insert = Insert(schema, table, values.toList)
        }

        case class Column(name: String, `type`: Column.Type.Value, constraints: List[Column.Constraint.Value]) {
          def generateCreateTableLine: String = s"$name ${`type`} ${constraints.mkString(" ")}"

        }

        object Column {

          object Type extends Enumeration {
            type Value
            val NONE, BOOLEAN, INTEGER, JSONB, SMALLINT, SERIAL, TEXT, TIMESTAMP = Value
          }

          object Constraint extends Enumeration {
            type Value
            val `NOT NULL` = Value("NOT NULL")
            val UNIQUE = Value("NOT NULL")
            val `PRIMARY KEY` = Value("PRIMARY KEY")
          }

          def builder(name: String, `type`: Column.Type.Value): ColumnBuilder = ColumnBuilder(name, `type`)
          def builder(name: String): ColumnBuilder = ColumnBuilder(name, Type.NONE)
        }

        case class ColumnBuilder(name: String, `type`: Column.Type.Value) extends AbstractBuilder[Column] {
          val constraints: ListBuffer[Column.Constraint.Value] = ListBuffer()

          def withConstraint(constraint: Column.Constraint.Value): ColumnBuilder = {
            constraints += constraint
            this
          }

          override def build: Column = Column(name,`type`, constraints.toList)
        }
      }
    }

    case class Schema(_name: String, tables : List[Table]) {
      val name= _name.trim.toLowerCase
      val tablesByName: Map[String, Table] = tables.map(table => table.name -> table).toMap

      def getTable(name: String): Table = tablesByName(name)

      def createSchema: Schema = {
        Connection.defaultConnection.executeUpdate(s"CREATE SCHEMA IF NOT EXISTS $name")
        this
      }

      def createTables: Schema = {
        tables.map(_.create)
        this
      }

      def create: Schema = {
        createSchema.createTables
      }

      def drop: Schema = {
        Connection.defaultConnection.executeUpdate(s"DROP SCHEMA $name CASCADE")
        this
      }

    }

    case class SchemaBuilder(name: String) extends AbstractBuilder[Schema]{

      val tables: ListBuffer[Table] = ListBuffer()

      def withTable(table: Table):SchemaBuilder = {
        tables += table
        this
      }

      def withTable(table: Schema.TableBuilder):SchemaBuilder = withTable(table.build)

      override def build: Schema = Schema(name, tables.toList)
    }
  }

  case class Database(name: String, schemas: List[Schema]) {

    private def createDatabase: Database = {
      //database.cre
      val rs = Connection.defaultConnection.execute(s"SELECT EXISTS(SELECT 1 from pg_database WHERE datname='$name')")
      if (rs.next()) {
        if (!rs.getBoolean("exists")) Connection.defaultConnection.executeUpdate(s"CREATE DATABASE $name")
      } else {
        System.out.println(s"failing executing SELECT EXISTS(SELECT 1 from pg_database WHERE datname='$name')")
      }
      this
    }

    def create: Database = {
      createDatabase.schemas.map(_.create)
      this
    }

    def drop: Database = {
      Connection.defaultConnection.executeUpdate(s"DROP DATABASE $name CASCADE")
      this
    }

    def dropSchemas: Database = {
      schemas.map(_.drop)
      this
    }
  }

  case class DatabaseBuilder(name: String) extends AbstractBuilder[Database] {

    val schemas: ListBuffer[Schema] = ListBuffer()

    def withSchema(schema: Schema): DatabaseBuilder = {
      this.schemas += schema
      this
    }

    def withSchema(schema: Database.SchemaBuilder): DatabaseBuilder = withSchema(schema.build)

    override def build: Database = Database(name, schemas.toList)
  }

}
