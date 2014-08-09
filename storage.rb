####################
### Requirements ###
####################

# waz-storage gem
# config/azure.yml file like :

  # development:
  #   account_name: account
  #   access_key: key
  # 
  # staging:
  #   account_name: account
  #   access_key: key
  #       
  # production:
  #   account_name: account
  #   access_key: key

module Paperclip
  module Storage
        
    module Azure1
      
      ##############################
      #### You have to set a Paperclip initializer to use this storage, for example

      # module Paperclip
      #   class Attachment
      #     def self.default_options
      #       @default_options ||= {
      #         :styles            => {},
      #         :processors        => [:thumbnail],
      #         :convert_options   => {},
      #         :default_url       => "/none.png",
      #         :default_style     => :original,
      #         :whiny             => Paperclip.options[:whiny] || Paperclip.options[:whiny_thumbnails],
      #         :storage           => :azure1,
      #         :path              => ":modelname/:attachment/:id/:style/:filename",        
      #         :azure_credentials => "#{RAILS_ROOT}/config/azure.yml",
      #         :azure_container   => "system",
      #         :azure_host_alias  => "azXXXXXX.vo.msecnd.net",
      #         :url               => ':azure_domain_url',
      #       }
      #     end    
      #   end
      # end

      ###### IMPORTANT #######
      ## If you use custom CNAME and CDN URL, be careful, Azure doesn't support HTTPS over this... trouble on the way.
      ## Check the second module Azure2 if you're finding yourself in that position
      ########################
      
      def self.extended base
        begin
          require 'waz-blobs'          
        rescue LoadError => e
          e.message << " (You may need to install the waz-storage gem)"
          raise e
        end

        base.instance_eval do
          @azure_credentials = parse_credentials(@options[:azure_credentials])
          @container         = @options[:azure_container] || @azure_credentials[:azure_container]
          @account_name      = @azure_credentials[:account_name]          
          @azure_host_alias  = @options[:azure_host_alias]
          @url               = ":azure_host_alias"
                    
          WAZ::Storage::Base.establish_connection!(:account_name => @azure_credentials[:account_name], :access_key => @azure_credentials[:access_key])
        end
        
        Paperclip.interpolates(:azure_host_alias) do |attachment, style|
          "//#{attachment.azure_host_alias}/#{attachment.container_name}/#{attachment.path(style).gsub(%r{^/}, "")}"
        end
        Paperclip.interpolates(:azure_path_url) do |attachment, style|
          "//#{attachment.account_name}.blob.core.windows.net/#{attachment.container_name}/#{attachment.path(style).gsub(%r{^/}, "")}"
        end
        Paperclip.interpolates(:azure_domain_url) do |attachment, style|
          "//#{attachment.account_name}.blob.core.windows.net/#{attachment.container_name}/#{attachment.path(style).gsub(%r{^/}, "")}"
        end
      end
      
      def custom_url(style = default_style, ssl = false)
        ssl ? self.url(style).gsub('http', 'https') : self.url(style)
      end
      
      def account_name
        @account_name
      end
      
      def container_name
        @container
      end

      def azure_host_alias
       @azure_host_alias
      end

      def parse_credentials creds
        creds = find_credentials(creds).stringify_keys
        (creds[RAILS_ENV] || creds).symbolize_keys
      end
      
      def exists?(style = default_style)
        if original_filename
          begin
            WAZ::Blobs::Container.find(container_name)[path(style)]
          rescue
            false
          end
        else
          false
        end
      end

      # Returns representation of the data of the file assigned to the given
      # style, in the format most representative of the current storage.
      def to_file style = default_style
        return @queued_for_write[style] if @queued_for_write[style]
        if exists?(style)
          file = Tempfile.new(path(style)) 
          file.write(WAZ::Blobs::Container.find(container_name)[path(style)].value)
          file.rewind
          file
        end
        return file
      end

      def flush_writes #:nodoc:
        @queued_for_write.each do |style, file|
          begin
            log("saving to Azure #{path(style)}")      
            WAZ::Blobs::Container.find(container_name).store(path(style), file.read, instance_read(:content_type), {:x_ms_blob_cache_control=>"max-age=315360000, public"})
          rescue
            log("error saving to Azure #{path(style)}")            
            ## If container doesn't exist we create it
            if WAZ::Blobs::Container.find(container_name).blank?
              WAZ::Blobs::Container.create(container_name).public_access = "blob"
              log("retryng saving to Azure #{path(style)}")            
              retry
            end
          end
        end
        @queued_for_write = {}
      end

      def flush_deletes #:nodoc:
        @queued_for_delete.each do |path|
          begin
            log("deleting #{path}")
            WAZ::Blobs::Container.find(container_name)[path].destroy!
          rescue
            log("error deleting #{path}")
          end
        end
        @queued_for_delete = []
      end
      
      def find_credentials creds
        case creds
        when File
          YAML::load(ERB.new(File.read(creds.path)).result)
        when String
          YAML::load(ERB.new(File.read(creds)).result)
        when Hash
          creds
        else
          raise ArgumentError, "Credentials are not a path, file, or hash."
        end
      end
      private :find_credentials
    end
      
    
    module Azure2

      ###################
      ## This second module is here to paliate the problem described with the first one (CDN + CNAME + SSL)
      
      ###### IMPORTANT #######
      ## You will have to send all your assets to Azure, images, js, and css too
      ## In order to do that check my waz-sync gem 
      ######
      ## This module will also save all your paperclip asset locally, which is a good thing i guess.
      ## We are doing that because on SSL page your server will deliver assets, and not the CDN, since HTTPS doesn't support CNAME
      ########################
      
      ######
      ## You have to set a Paperclip initializer to use this storage, for example
      
      # module Paperclip
      #   class Attachment
      #     def self.default_options
      #       @default_options ||= {
      #         :styles            => {},
      #         :processors        => [:thumbnail],
      #         :convert_options   => {},
      #         :default_url       => "/none.png",
      #         :default_style     => :original,
      #         :whiny             => Paperclip.options[:whiny] || Paperclip.options[:whiny_thumbnails],
      #         :storage           => :azure2,
      #         :azure_credentials => "#{RAILS_ROOT}/config/azure.yml",
      #         :azure_container   => "system",
      #         :path              => "datadb/:modelname/:attachment/:id/:style/:filename",
      #         :url               => '/system/datadb/:modelname/:attachment/:id/:style/:filename'
      #       }
      #     end    
      #   end
      # end
      
      ######
      ## You also have to set the asset_host configuration
      
      # ActionController::Base.asset_host = Proc.new { |source, request|
      #   if !request.ssl?
      #     "http://azXXXXX.vo.msecnd.net"
      #   else
      #     "#{request.protocol}#{request.host_with_port}"
      #   end
      # }
          
      def self.extended base
        begin
          require 'waz-blobs'          
        rescue LoadError => e
          e.message << " (You may need to install the waz-storage gem)"
          raise e
        end

        base.instance_eval do
          @azure_credentials = parse_credentials(@options[:azure_credentials])
          @container         = @options[:azure_container] || @azure_credentials[:azure_container]
          WAZ::Storage::Base.establish_connection!(:account_name => @azure_credentials[:account_name], :access_key => @azure_credentials[:access_key])
        end
      end
      
      def custom_url(style = default_style, ssl = false)
        ssl ? self.url(style).gsub('http', 'https') : self.url(style)
      end
      
      
      def container_name
        @container
      end

      def parse_credentials creds
        creds = find_credentials(creds).stringify_keys
        (creds[RAILS_ENV] || creds).symbolize_keys
      end
      
      def exists?(style = default_style)
        if original_filename
          begin
            WAZ::Blobs::Container.find(container_name)[path(style)]
          rescue
            false
          end
        else
          false
        end
      end

      # Returns representation of the data of the file assigned to the given
      # style, in the format most representative of the current storage.
      def to_file style = default_style
        return @queued_for_write[style] if @queued_for_write[style]
        if exists?(style)
          file = Tempfile.new(path(style)) 
          file.write(WAZ::Blobs::Container.find(container_name)[path(style)].value)
          file.rewind
          file
        end
        return file
      end

      def flush_writes #:nodoc:
        @queued_for_write.each do |style, file|
          begin
            log("saving to Azure #{path(style)}")      
            WAZ::Blobs::Container.find(container_name).store(path(style), file.read, instance_read(:content_type), {:x_ms_blob_cache_control=>"max-age=315360000, public"})
          rescue
            log("error saving to Azure #{path(style)}")            
            ## If container doesn't exist we create it
            if WAZ::Blobs::Container.find(container_name).blank?
              WAZ::Blobs::Container.create(container_name).public_access = "blob"
              log("retryng saving to Azure #{path(style)}")            
              retry
            end
          end
        end
        fs_flush_writes
        @queued_for_write = {}
      end

      def flush_deletes #:nodoc:
        @queued_for_delete.each do |path|
          begin
            log("deleting #{path}")
            WAZ::Blobs::Container.find(container_name)[path].destroy!
          rescue
            log("error deleting #{path}")
          end
        end
        fs_flush_deletes
        @queued_for_delete = []
      end
      
      def fs_flush_writes #:nodoc:
        @queued_for_write.each do |style_name, file|
          file.close
          FileUtils.mkdir_p(File.dirname("#{Rails.root}/public/system/" + path(style_name)))
          log("saving to FS #{"/system/" + path(style_name)}")
          FileUtils.mv(file.path, "#{Rails.root}/public/system/" + path(style_name))
          FileUtils.chmod(0644, "#{Rails.root}/public/system/" + path(style_name))        
        end
      end

      def fs_flush_deletes #:nodoc:
        @queued_for_delete.each do |path|
          begin
            log("deleting to FS #{"/system/" + path}")
            FileUtils.rm("#{Rails.root}/public/system/" + path) if File.exist?("#{Rails.root}/public/system/" + path)            
          rescue Errno::ENOENT => e
            # ignore file-not-found, let everything else pass
          end
          begin
            while(true)
              path = File.dirname("#{Rails.root}/public/system/" + path)
              FileUtils.rmdir("#{Rails.root}/public/system/" + path)
            end
          rescue Errno::EEXIST, Errno::ENOTEMPTY, Errno::ENOENT, Errno::EINVAL, Errno::ENOTDIR
            # Stop trying to remove parent directories
          rescue SystemCallError => e
            log("There was an unexpected error while deleting directories: #{e.class}")
            # Ignore it
          end
        end
      end
      
      def find_credentials creds
        case creds
        when File
          YAML::load(ERB.new(File.read(creds.path)).result)
        when String
          YAML::load(ERB.new(File.read(creds)).result)
        when Hash
          creds
        else
          raise ArgumentError, "Credentials are not a path, file, or hash."
        end
      end
      private :find_credentials
    end
    
  end
end