# ## Assignment
#
# The goal of the assignment is to read USD-to-EUR exchange rates from ECB,
# store them in a database, and implement a Ruby API that will return the
# value in EUR of a given amount of USD on a given day (since the start of the
# year 2000).
#
# Fetching is a mundane administrative task that will be performed in a rake
# task (`rake ecb:fetch`). This way we make it _pluggable_, so the user could
# opt to perform the same task via curl, for example. This code will not care
# as long as the file is present on the filesystem.

module Ecb
  require "csv"
  require "bigdecimal"

  # The next step is parsing the CSV. We convert each row to a struct that we
  # define for this purpose.

  ExchangeRate = Struct.new(:from, :to, :date, :value)

  # Reading the file this way will result in allocating an array of exchange
  # rates in memory. There are alternative approaches that allow us to avoid
  # allocation, for example using plain `foreach` and skipping the enumerable
  # methods in favour of persisting the rows immediately to a database. Given
  # that the CSV file is about 4000 lines long and that its growth is
  # predictable (less than 1 line per day on average) and given that the
  # requirements for this code did not specify any restrictions regarding
  # resource usage, we think that this is a worthy tradeoff. In exchange we
  # get code that is more readable and also less coupled (parsing and
  # persisting are separate).

  module Parse
    module_function

    def read(filename)
      rows = CSV.foreach(filename)
      rows = reject_headers(rows)

      rates = parse_rows(rows)

      rates.compact
    end

    # As ruby's CSV class does not support multiline headers, we attempt to
    # reject irrelevant rows by checking if the first cell contains a date
    # (altough the date could still be invalid at this point).
    def reject_headers(rows)
      rows.select { |row|
        row[0] =~ /\A\d{4}-\d{2}-\d{2}/
      }
    end

    def parse_rows(rows)
      rows.map { |(date, value)|
        date = parse_date(date)
        value = parse_value(value)

        next if date == :invalid || value == :invalid

        ExchangeRate.new(:usd, :eur, date, value)
      }
    end

    # We try not to crash in case the date is invalid. There were no such rows
    # in the downloaded CSV, but since we don't control the web service, we
    # program defensively.
    def parse_date(string)
      Date.parse(string)
    rescue ArgumentError
      :invalid
    end

    # Handle some cases when there is a given date, but no value is availalbe
    # (the cell contains a `-`).
    def parse_value(string)
      BigDecimal(string)
    rescue ArgumentError
      :invalid
    end
  end

  # ## Choosing a database
  #
  # Given the requirements, it seems that we need some kind of key-value
  # store. Except for fetching exchange rates by date (the key), the only
  # other required functionality is finding the nearest previous key, in case
  # there is no exchange rate for the requested date (due to weekends,
  # holidays, etc.). A more expressive query language like SQL would allow us
  # to achieve this. A naive solution would be:
  #
  #     This will return value for 2017-06-02:
  #     SELECT value FROM rates WHERE date <= '2017-06-04' LIMIT 1;
  #      value
  #     --------
  #      1.1217
  #     (1 row)
  #
  # A different approach with a simpler key-value database like Redis would be
  # to just blindly check for a previous day's value unless one is found. A
  # less naive approach would be to construct a data structure that contains a
  # pointer to the previous available value (akin to a linked list).
  #
  # Because the assignment does not specify any expected performance
  # characteristics, we will opt for a simpler storage solution. In fact,
  # given that the reviewer will probalby want to run the solution on their
  # machine, it makes sense to use a solution that will be the easisest to set
  # up.
  #
  # `PStore` from ruby's standard library seems like a good fit as it does not
  # have any external dependencies. Ruby objects are persisted to PStore
  # through marshalling, which could be an issue when moving a database
  # between machines with different architectures and/or ruby versions. As the
  # database can be rebuilt prior to use, we don't expect the need to share
  # it.

  require "pstore"

  # To take advantage of PStore we create a simple wrapper.

  class Persistence
    def initialize(path:)
      @store = PStore.new(path)
    end

    # We try to retrieve the stored value for a given date. If that fails, we
    # try to retrieve the value for the previous day. The requirement to only
    # return dates since the year 2000 conveniently serves as a recursion
    # terminator. In practice, the gaps between dates are quite narrow, so
    # the recursion will go a few levels deep at most.
    def retrieve(date)
      if date.year < 2000
        raise ArgumentError, "Data not available before 2000-01-01"
      end

      value =
        read do |store|
          store[date]
        end

      value || retrieve(date.prev_day)
    end

    def save(rates)
      write do |store|
        rates.each do |rate|
          store[rate.date] = rate.value
        end
      end
    end

    private

    def read
      @store.transaction(true) do
        yield(@store)
      end
    end

    def write
      @store.transaction do
        yield(@store)
      end
    end
  end

  # Finally, the interface to perform conversion.

  class Conversion
    attr_reader :exchange

    def initialize(exchange)
      @exchange = exchange
    end

    def usd_to_eur(amount, date)
      rate = exchange.retrieve(date)
      BigDecimal(amount) / rate
    end
  end

  # ### Wiring it all together
  #
  # The code will only be run if the file is executed directly, e.g.:
  #
  #     ruby lib/ecb.rb rates/usdeur.csv

  if File.identical?(__FILE__, $0)
    input_file = $ARGV[0]

    unless File.readable?(input_file)
      abort "error: unable to read exchange rates from file #{input_file}"
    end

    # The code can be safely run multiple times, always resulting in the same
    # `rates.pstore` file written on disk.
    rates = Parse.read(input_file)
    exchange = Persistence.new(path: "rates.pstore")
    exchange.save(rates)
    conversion = Conversion.new(exchange)

    # Returns what 120 USD was in euros on March 5, 2011.
    puts "%.2f€" % conversion.usd_to_eur(120, Date.civil(2011, 3, 5))
  end
end

# ## Performance
#
# A simple benchmark with `benchmark/ips` showed poor results, averaging at 40
# reads per seconds on my machine. This is due to the fact that we open a new
# transaction for every read, which is a performance killer for PStore. A
# quick modification that allowed the benchmark to run entirely within a
# single transaction averaged at around 2 million reads per second.
#
# This means that in order to use this code in a production environment with
# significant loads (e.g. the current performance might be adequate for a
# command-line utility, for example), the code would have to be adapted to
# open a transaction before peforming a sequence of reads. On the other side,
# we can't keep the transaction open the entire time if we ever want to update
# the exchange rates without interrupting the application.
#
# This solution is therefore, like most things in software and in life, a
# tradeoff.
