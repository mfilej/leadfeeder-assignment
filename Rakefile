require "rocco/tasks"
Rocco::make "doc/", "lib/*.rb", stylesheet: "../style.css"

desc "Generate documentation (literate programming style)"
task doc: :rocco

namespace :ecb do
  desc "Fetch USD-to-EUR exchange rates from ECB"
  task :fetch do
    uri = URI(
      "http://sdw.ecb.europa.eu/quickviewexport.do" \
      "?SERIES_KEY=120.EXR.D.USD.EUR.SP00.A&type=c sv"
    )
    response = Net::HTTP.get_response(uri)
    
    case response
    when Net::HTTPSuccess
      FileUtils.mkdir_p("rates")
      File.open("rates/usdeur.csv", "w") do |f|
        f.write(response.body)
      end
    else
      abort "Unable to fetch exchange rates (#{response.class})"
    end
  end
end
