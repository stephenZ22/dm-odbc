module ODBCAdapter
  module Adapters
    # A default adapter used for databases that are no explicitly listed in the
    # registry. This allows for minimal support for DBMSs for which we don't
    # have an explicit adapter.
    class DmODBCAdapter < ActiveRecord::ConnectionAdapters::ODBCAdapter
      PRIMARY_KEY = 'int IDENTITY (1, 1) NOT NULL'.freeze

      class BindSubstitution < Arel::Visitors::ToSql
        include Arel::Visitors::BindVisitor
      end

      # 达梦 true和false的值
      def quoted_true
        "'1'".freeze
      end

      def unquoted_true
        "1".freeze
      end

      def quoted_false
        "'0'".freeze
      end

      def unquoted_false
        "0".freeze
      end

      # Using a BindVisitor so that the SQL string gets substituted before it is
      # sent to the DBMS (to attempt to get as much coverage as possible for
      # DBMSs we don't support).
      def arel_visitor
        BindSubstitution.new(self)
      end

      # Explicitly turning off prepared_statements in the null adapter because
      # there isn't really a standard on which substitution character to use.
      def prepared_statements
        false
      end

      # Turning off support for migrations because there is no information to
      # go off of for what syntax the DBMS will expect.
      def supports_migrations?
        true
      end

      def supports_json?
        true
      end

      # support dm
      def rename_column(table_name, column_name, new_column_name)
        column = column_for(table_name, column_name)
        current_type = column.native_type
        current_type << "(#{column.limit})" if column.limit
        execute_run("ALTER TABLE #{quote_table_name(table_name)} RENAME COLUMN #{quote_column_name(column_name)} TO #{quote_column_name(new_column_name)}")
      end

      def change_column(table_name, column_name, type, options = {})
        unless options_include_default?(options)
          options[:default] = column_for(table_name, column_name).default
        end
        change_column_sql = "ALTER TABLE #{quote_table_name(table_name)} MODIFY #{quote_column_name(column_name)} #{type_to_sql(type, limit: options[:limit], precision: options[:precision], scale: options[:scale])}"
        add_column_options!(change_column_sql, options)
        execute_run(change_column_sql)
      end

      def change_column_default(table_name, column_name, default)
        execute_run("ALTER TABLE #{quote_table_name(table_name)} ALTER COLUMN #{quote_column_name(column_name)} SET DEFAULT #{quote(default)}")
      end

      def rename_index(_table_name, old_name, new_name)
        execute_run("ALTER INDEX #{quote_column_name(old_name)} RENAME TO #{quote_table_name(new_name)}")
      end

      def remove_index(_table_name, index_name)
        execute_run("DROP INDEX #{index_name[:name]}" )
      end

      def rename_table(table_name, new_name)
        execute_run("ALTER TABLE #{quote_table_name(table_name)} RENAME TO #{quote_table_name(new_name)}")
      end

      def options_include_default?(options)
        options.include?(:default) && !options[:default].nil?
      end

      # 转换添加列的选项
      def add_column_options!(sql, options)
        sql << " DEFAULT #{options[:default]}" if options_include_default?(options)
        if options[:null] == false
          sql << " NOT NULL"
        end
        if options[:auto_increment] == true
          sql << " AUTO_INCREMENT"
        end
        if options[:primary_key] == true
          sql << " PRIMARY KEY"
        end
        sql
      end

      # modify mediumtext的字段,需要改为text
      def type_to_sql(type, limit: nil, precision: nil, scale: nil, **) # :nodoc:
        return 'text' if type.to_s == 'mediumtext'

        return 'bit' if type.to_s == 'integer' && limit == 1
        
        super
      end

      protected

      # 返回插入数据的id,否则rails create后对象id为nil
      def last_inserted_id(_result)
        select_value('select @@IDENTITY').to_i
      end
    end
  end
end
