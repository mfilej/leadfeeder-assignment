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
  # define for this purpose:

  ExchangeRate = Struct.new(:from, :to, :date, :value)

  # We skip `HEADER_LINES` and then proceed with reading the remaining rows,
  # converting the exchange rate to a number. Notice that in order to avoid
  # loosing precision, we explicitly convert strings into `BigDecimal`
  # instances instead of `Float`s.
  #
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
    HEADER_LINES = 5

    module_function
    def read(filename)
      rows = CSV.foreach(filename)
      rows = rows.drop(HEADER_LINES)

      rows.map { |(date, value)|
        date = parse_date(date)
        value = parse_value(value)

        next if value == :not_available

        ExchangeRate.new(:usd, :eur, date, value)

      }.compact
    end

    def parse_date(string)
      Date.parse(string)
    rescue ArgumentError
      abort "Unable to parse date: #{string.inspect}"
    end

    def parse_value(string)
      BigDecimal(string)
    rescue ArgumentError
      :not_available
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
  # to just blindly check for a previous day's value unless one is found.
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
  # it, so PStore seems to be a good fit here.

  require "pstore"

  class Persistence
    def initialize(path: "rates.pstore")
      @store = PStore.new(path)
    end

    def retrieve(date)
      read do |store|
        store[date]
      end
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
end
