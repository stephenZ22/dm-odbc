module ODBCAdapter
  module DatabaseStatements
    # ODBC constants missing from Christian Werner's Ruby ODBC driver
    SQL_NO_NULLS = 0
    SQL_NULLABLE = 1
    SQL_NULLABLE_UNKNOWN = 2

    # Executes the SQL statement in the context of this connection.
    # Returns the number of rows affected.
    # 返回行数
    def execute_run(sql, name = nil, binds = [])
      sql  = change_sql(sql)
      sqls = change_modify_sql(sql)
      result = nil
      sqls.each do |sql|
        result = log(sql, name) do
          if prepared_statements
            @connection.do(sql, *prepared_binds(binds))
          else
            @connection.do(sql)
          end
        end
      end
      result
    end

    # 返回结果，结果数组，没有健值映射
    def execute(sql, name = 'SQL', binds = [], prepare: false) # rubocop:disable Lint/UnusedMethodArgument
      # 这里不需要change_modify_sql，因为这里会返回i数组结果的，而modify不需要返回
      sql  = change_sql(sql)
      log(sql, name) do
        stmt =
          if prepared_statements
            @connection.run(sql, *prepared_binds(binds))
          else
            @connection.run(sql)
          end

        columns = stmt.columns
        values  = stmt.to_a
        stmt.drop
        values = dbms_type_cast(columns.values, values)
        column_names = columns.keys.map { |key| format_case(key) }
        values
        # ActiveRecord::Result.new(column_names, values)
      end
    end

    # Executes +sql+ statement in the context of this connection using
    # +binds+ as the bind substitutes. +name+ is logged along with
    # the executed +sql+ statement.
    # 返回结果，健值映射数组
    def exec_query(sql, name = 'SQL', binds = [], prepare: false) # rubocop:disable Lint/UnusedMethodArgument
      # 这里不需要change_modify_sql，因为这里会返回i数组结果的，而modify不需要返回
      sql  = change_sql(sql)
      log(sql, name) do
        stmt =
          if prepared_statements
            @connection.run(sql, *prepared_binds(binds))
          else
            @connection.run(sql)
          end

        columns = stmt.columns
        values  = stmt.to_a
        stmt.drop
        values = dbms_type_cast(columns.values, values)
        column_names = columns.keys.map { |key| format_case(key) }
        ActiveRecord::Result.new(column_names, values)
      end
    end

    # Executes delete +sql+ statement in the context of this connection using
    # +binds+ as the bind substitutes. +name+ is logged along with
    # the executed +sql+ statement.
    def exec_delete(sql, name, binds)
      execute_run(sql, name, binds)
    end
    alias exec_update exec_delete

    # Begins the transaction (and turns off auto-committing).
    def begin_db_transaction
      @connection.autocommit = false
    end

    # Commits the transaction (and turns on auto-committing).
    def commit_db_transaction
      @connection.commit
      @connection.autocommit = true
    end

    # Rolls back the transaction (and turns on auto-committing). Must be
    # done if the transaction block raises an exception or returns false.
    def exec_rollback_db_transaction
      @connection.rollback
      @connection.autocommit = true
    end

    # Returns the default sequence name for a table.
    # Used for databases which don't support an autoincrementing column
    # type, but do support sequences.
    def default_sequence_name(table, _column)
      "#{table}_seq"
    end

    private

    # def change_sql(sql)
    #   # # 替换json
    #   # json_column = new_sql.match(/(?<=\s)[^\s]+(?=\sjson)/).to_s
    #   # new_sql = new_sql.gsub("#{json_column} json", "#{json_column} text CHECK (#{json_column} IS JSON)") if json_column.present?
    #   # new_sql
    # end

    # modify sql 修改 
    # 由于程序无法同时执行多条sql语句，所以返回数组，依次执行
    def change_modify_sql(sql)
      return [sql] if sql.match(/ALTER\s.*\sMODIFY\s.*\s/i).blank?

      table_name      = sql.match(/ALTER TABLE (.*) MODIFY/)[1]
      column_name     = sql.match(/MODIFY\s([^\s]+)/)[1]
      new_column_name = column_name.sub(/\"$/, '1"')
      type_name       = sql.match(/MODIFY\s[^\s]+\s([^\s]+)/)[1]
      option          = sql.match(/MODIFY\s[^\s]+\s[^\s]+\s(.*)/)&.[](1)

      [
        "alter table #{table_name} add #{new_column_name} #{type_name} #{option};",
        "update #{table_name} set #{new_column_name}=#{column_name};",
        "alter table #{table_name} drop column #{column_name};",
        "alter table #{table_name} rename column #{new_column_name} to #{column_name};"
      ]        
    end

    def change_sql(sql)
      sql = change_for_update_sql(sql)
      sql = change_increment_sql(sql)
      sql
    end

    # 修改for update 语句
    def change_for_update_sql(sql)
      return sql if sql.match(/SELECT.* FOR\s+UPDATE/i).blank?

      "#{sql};commit;"
    end

    # 修改自增语句（自增自断不允许插入数据，需要修改语句）
    def change_increment_sql(sql)
      return sql if sql.match(/INSERT\s*INTO.*\(\"ID\",.*VALUES.*/i).blank?

      table_name = sql.match(/INSERT\s*INTO\s*([^\s]+)/)[1]
      "SET IDENTITY_INSERT #{table_name} ON;#{sql};SET IDENTITY_INSERT #{table_name} OFF;"
    end

    # A custom hook to allow end users to overwrite the type casting before it
    # is returned to ActiveRecord. Useful before a full adapter has made its way
    # back into this repository.
    def dbms_type_cast(_columns, values)
      values
    end

    # Assume received identifier is in DBMS's data dictionary case.
    def format_case(identifier)
      if database_metadata.upcase_identifiers?
        identifier =~ /[a-z]/ ? identifier : identifier&.downcase
      else
        identifier
      end
    end

    # In general, ActiveRecord uses lowercase attribute names. This may
    # conflict with the database's data dictionary case.
    #
    # The ODBCAdapter uses the following conventions for databases
    # which report SQL_IDENTIFIER_CASE = SQL_IC_UPPER:
    # * if a name is returned from the DBMS in all uppercase, convert it
    #   to lowercase before returning it to ActiveRecord.
    # * if a name is returned from the DBMS in lowercase or mixed case,
    #   assume the underlying schema object's name was quoted when
    #   the schema object was created. Leave the name untouched before
    #   returning it to ActiveRecord.
    # * before making an ODBC catalog call, if a supplied identifier is all
    #   lowercase, convert it to uppercase. Leave mixed case or all
    #   uppercase identifiers unchanged.
    # * columns created with quoted lowercase names are not supported.
    #
    # Converts an identifier to the case conventions used by the DBMS.
    # Assume received identifier is in ActiveRecord case.
    def native_case(identifier)
      if database_metadata.upcase_identifiers?
        identifier =~ /[A-Z]/ ? identifier : identifier.upcase
      else
        identifier
      end
    end

    # Assume column is nullable if nullable == SQL_NULLABLE_UNKNOWN
    def nullability(col_name, is_nullable, nullable)
      not_nullable = (!is_nullable || !nullable.to_s.match('NO').nil?)
      result = !(not_nullable || nullable == SQL_NO_NULLS)

      # HACK!
      # MySQL native ODBC driver doesn't report nullability accurately.
      # So force nullability of 'id' columns
      col_name == 'id' ? false : result
    end

    def prepared_binds(binds)
      prepare_binds_for_database(binds).map { |bind| _type_cast(bind) }
    end

    # HACK！
    # support activerecord 5.1
    def prepare_binds_for_database(binds)
      binds.map(&:value_for_database)
    end
  end
end
