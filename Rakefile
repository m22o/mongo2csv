require "pry"
require "mongo"
require "csv"
require 'dotenv'
Dotenv.load

QUERY_NUMBER = 10000
SELECT_COUNTRY = ["Japan","United States","France","Italy","Spain","United Kingdom","Germany","Brazil","Australia","Russia","Canada","Croatia","China"]
MAX_NUM = 65000

def export_csv(rooms,country,loop_count)
  types = ["id","star_rating","amenities","country","property_type","reviews_count","room_type","star_rating"]
  clean_data = []
  rooms.each do |result|
    clean_data << [
      result[types[0]],
      result["payload"][types[1]],
      result["payload"][types[2]].to_s,
      result["payload"][types[3]],
      result["payload"][types[4]],
      result["payload"][types[5]],
      result["payload"][types[6]],
      result["payload"][types[7]],
      result["payload"][types[8]],
    ]
  end

  CSV.open("airbnb_#{country}_#{loop_count+1}.csv",'w') do |data|
    data << types
    clean_data.each do |d|
      data << d
    end
  end
end
namespace :airbnb do
  desc "convert_csv"
  task "to_csv"  do
    db = Mongo::Client.new([ENV["HOST"]], :database => ENV["DB_NAME"])
    #rooms = db[ENV["MONGO_COLLECTION"]].find({'payload.country':SELECT_COUNTRY}).limit(QUERY_NUMBER).to_a
    SELECT_COUNTRY.each do |country|
      rooms = db[ENV["MONGO_COLLECTION"]].find({'payload.country':country}).to_a
      loop_count = rooms.size/MAX_NUM + 1
      loop_count.times do |loop_count|
        start_num = loop_count*MAX_NUM
        end_num = (loop_count+1)*MAX_NUM - 1
        export_csv(rooms[start_num..end_num],country,loop_count)
      end
    end
  end
end
