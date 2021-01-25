# frozen_string_literal: true

require 'logstash/inputs/base'
require 'logstash/namespace'
require 'stud/interval'
require 'aws-sdk'
require 'logstash/inputs/rds/patch'
require 'logstash/plugin_mixins/aws_config'
require 'time'

Aws.eager_autoload!

class LogStash::Inputs::Rds < LogStash::Inputs::Base
  include LogStash::PluginMixins::AwsConfig::V2

  config_name 'rds'
  milestone 1
  default :codec, 'plain'

  config :instance_name, validate: :string, required: true
  config :log_file_name, validate: :string, required: true
  config :polling_frequency, validate: :number, default: 600
  config :sincedb_path, validate: :string, default: nil
  config :number_of_lines, validate: :number, default: 10_000

  def register
    @logger.info 'Registering RDS input', region: @region, instance: @instance_name, log_file: @log_file_name,
                                          number_of_lines: @number_of_lines
    @database = Aws::RDS::DBInstance.new @instance_name, aws_options_hash

    path = @sincedb_path || File.join(ENV['HOME'],
                                      '.sincedb_' + Digest::MD5.hexdigest("#{@instance_name}+#{@log_file_name}"))
    @sincedb = SinceDB::File.new path
  end

  def run(queue)
    @thread = Thread.current
    Stud.interval(@polling_frequency) do
      original_time = @sincedb.last_written
      time = original_time
      @logger.debug "finding #{@log_file_name} for #{@instance_name} starting #{Time.at(time / 1000)} (#{time}) markers=#{@sincedb.markers}"
      begin
        logfiles = @database.log_files(
          filename_contains: @log_file_name,
          file_last_written: time
        )
        logfiles.each do |logfile|
          time = logfile.last_written if logfile.last_written > time
          @logger.debug "downloading #{logfile.name} for #{@instance_name}"
          more = true
          while more
            response = logfile.download(marker: @sincedb.marker(logfile.name), number_of_lines: @number_of_lines)
            (response[:log_file_data]&.lines || []).each do |line|
              @codec.decode(line) do |event|
                decorate event
                event.set 'rds_instance', @instance_name
                event.set 'log_file', @log_file_name
                queue << event
              end
            end
            @logger.debug "instance=#{@instance_name} current_marker=#{@sincedb.marker(logfile.name)} original_time=#{Time.at(original_time / 1000)} additional_data_pending=#{response[:additional_data_pending]} marker=#{response[:marker]}, file=#{logfile.name}"
            more = response[:additional_data_pending]
            @sincedb.update_marker(logfile.name, response[:marker])
          end
        end
        @sincedb.last_written = time
      rescue Aws::RDS::Errors::ServiceError => e
        # the next iteration will resume at the same location
        @logger.warn 'caught AWS service error'
        @logger.warn e.message
        @logger.warn e.backtrace.join("\n\t")
      ensure
        @sincedb.store
      end
    end
  end

  def stop
    Stud.stop! @thread
  end

  module SinceDB
    class File
      attr_writer :last_written

      def initialize(file)
        @db = file
        @last_written = nil
        @markers = nil
      end

      def last_written
        @last_written || read && @last_written
      end

      def marker(filename)
        markers[filename2datetime(filename)]
      end

      def update_marker(filename, value)
        markers[filename2datetime(filename)] = value
      end

      def markers
        @markers || read && @markers
      end

      def store
        ::File.open(@db, 'w') do |file|
          file.puts @last_written
          file.puts @markers.to_json
        end
      end

      def read
        @last_written, @markers = if ::File.exist?(@db)
                                    content = ::File.read(@db).chomp.split("\n")
                                    content.empty? ? default_now : parse_content(content)
                                  else
                                    default_never
                                  end
      end

      private

      def parse_content(content)
        [content[0].to_i, parse_markers(content[1])]
      end

      def parse_markers(value)
        JSON.parse(value || '')
      rescue JSON::ParserError
        default_markers
      end

      def default_now
        [Time.new.to_i * 1000, default_markers]
      end

      def default_markers
        Hash.new('0')
      end

      def default_never
        [0, default_markers]
      end

      def filename2datetime(name)
        name.match(/(\d{4})-(\d{2})-(\d{2})-(\d{2})$/)[0]
      end
    end
  end
end
