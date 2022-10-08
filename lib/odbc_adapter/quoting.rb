module ODBCAdapter
  module Quoting
    # Quotes a string, escaping any ' (single quote) characters.
    def quote_string(string)
      string.gsub(/\'/, "''")
    end

    # Returns a quoted form of the column name.
    def quote_column_name(name)
      # 字段加了引号后，会区分大小写，所以这里要使用大写
      name = name.upcase.to_s
      quote_char = database_metadata.identifier_quote_char.to_s.strip

      # 注意： 这里会传进来类似admin_audit_logs.operation_category的字段，以后要在外层修改
      name.split('.').map{|name| "#{quote_char.chr}#{name}#{quote_char.chr}"}.join('.')
      # "#{quote_char.chr}#{name}#{quote_char.chr}"
      # return name if quote_char.length.zero?
      # quote_char = quote_char[0]

      # # Avoid quoting any already quoted name
      # return name if name[0] == quote_char && name[-1] == quote_char

      # # If upcase identifiers, only quote mixed case names.
      # if database_metadata.upcase_identifiers?
      #   return name unless name =~ /([A-Z]+[a-z])|([a-z]+[A-Z])/
      # end

      # "#{quote_char.chr}#{name}#{quote_char.chr}"
    end

    # Ideally, we'd return an ODBC date or timestamp literal escape
    # sequence, but not all ODBC drivers support them.
    def quoted_date(value)
      if value.acts_like?(:time)
        zone_conversion_method = ActiveRecord::Base.default_timezone == :utc ? :getutc : :getlocal

        if value.respond_to?(zone_conversion_method)
          value = value.send(zone_conversion_method)
        end
        value.strftime('%Y-%m-%d %H:%M:%S') # Time, DateTime
      else
        value.strftime('%Y-%m-%d') # Date
      end
    end
  end
end
