module ODBCAdapter
  module Adapters
    # A default adapter used for databases that are no explicitly listed in the
    # registry. This allows for minimal support for DBMSs for which we don't
    # have an explicit adapter.
    class NullODBCAdapter < ActiveRecord::ConnectionAdapters::ODBCAdapter
      PRIMARY_KEY = 'int IDENTITY (1, 1) NOT NULL'.freeze

      class BindSubstitution < Arel::Visitors::ToSql
        include Arel::Visitors::BindVisitor
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
        true
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
        execute("ALTER TABLE #{table_name} RENAME COLUMN #{column_name} TO #{new_column_name}")
      end

      def change_column(table_name, column_name, type, options = {})
        unless options_include_default?(options)
          options[:default] = column_for(table_name, column_name).default
        end

        change_column_sql = "ALTER TABLE #{table_name} MODIFY #{column_name} #{type_to_sql(type, limit: options[:limit], precision: options[:precision], scale: options[:scale])}"
        # TODO: add_column_options! 需重新复写
        # add_column_options!(change_column_sql, options)
        execute(change_column_sql)
      end

      def change_column_default(table_name, column_name, default)
        execute("ALTER TABLE #{table_name} ALTER COLUMN #{column_name} SET DEFAULT #{quote(default)}")
      end

      def rename_index(_table_name, old_name, new_name)
        execute("ALTER INDEX #{quote_column_name(old_name)} RENAME TO #{quote_table_name(new_name)}")
      end

      def remove_index(_table_name, index_name)
        execute("DROP INDEX #{index_name[:name]}" )
      end

      def rename_table(table_name, new_name)
        execute("ALTER TABLE #{quote_table_name(table_name)} RENAME TO #{quote_table_name(new_name)}")
      end

      def options_include_default?(options)
        if options.include?(:default) && options[:default].nil?
          if options.include?(:column) && options[:column].native_type =~ /timestamp/i
            options.delete(:default)
          end
        end
        super(options)
      end
    end
  end
end
