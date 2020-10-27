#!/usr/local/bin/perl
BEGIN 
{
  BEGIN
  {
    push @INC, "C:/PERLMODULES/","../PERLMODULES/", $ENV{PERLMODULES},"$ENV{HOME}/PERLMODULES/";
  }
  
  use AbstractHandler;
  use Logger::Logger;
  use Cwd;
  use File::Basename;
  
  $logdir = defined $ENV{APPL_LOG} ?  $ENV{APPL_LOG} : getcwd() . "/log/";
  $gCurrent_date = AbstractHandler::getCurrentDate();
  $gCurrentTime = AbstractHandler::getCurrentTime();
  
  $logger       = new Logger( DEBUG , basename($0,'.pl') . "_".$gCurrent_date."_" . $gCurrentTime . ".log" , $logdir);
  $loggerSTDOUT = new Logger(DEBUG);
  
  
  $USAGE="USAGE: $0 -s <source dir> -d <destination dir> [-e 16.MTS -e .log,.txt] [-v verbose]";
}

use Digest::MD5 qw(md5_hex);
use File::Copy;
use File::Basename;
use File::Path qw(mkpath);
use File::stat;
use Getopt::Long;
use URI::file;
use URI::Escape;
use Data::Dumper;
my $gCopyCounter = 0;
my $gTotalVolume = 0;

$Data::Dumper::Terse = 1;       
$Data::Dumper::Indent = 0;
$Data::Dumper::Purity = 1;  
    
$| = 1;


#sub _executeCommand($) {
# my ($command) = @_;
#
# $logger->Trace("Execute $command");
#
# my $ret = system($command);
# if ( ( $ret >> 8 ) != 0 ) {
#   return $logger->Error(
#     "Execution of  [$command] failed with code : [$ret] ,$!");
# }
# else {
#   return 1;
# }
#}

sub hdfs_escape( $ ) 
{ 
   my $value = shift;
   $value =~ s/([\[\]\(\)])/\\$1/g;
   return $value;
}
#########################################################################################################
sub _validate ( $ )
{
  my $size = stat(shift)->size;
  
  if ( $size > 0 )
  {
    $gCopyCounter++;
    $gTotalVolume += $size;
    
    return $size; 
  }
 
  return undef;
}  
#########################################################################################################
sub _match( $$ )
{
    my $findWhere = $_[0];
    my @patterns = @{$_[1]};
    
    my @compiledPatters;
    
    my $found = undef;
    
    push @compiledPatters , qr($_) for @patterns;
    
    foreach (@compiledPatters)
    {
       $found = 1;
       last if ( $findWhere =~ /$_/ ) &&  defined $_ ;
       $found = undef;  #not found
    }
    
    return $found; # 1 found or undef not matched
}
#########################################################################################################
sub _handler ( $$ )
{
  my ( $i_file_name , $params ) = @_;
    
  my $io_hash_ref = ${$params}[0];
  my $ignoreList_ref = ${$params}[1];
  my $root_folder = ${$params}[2];
  
  #add to hash only of file name not in ignore list
  if ( ! _match( basename( $i_file_name ) , $ignoreList_ref ) )
  {
    #need to remove a single / because during sync operation to create a full path used patter $root_path / $relative_path
    my ( $dymmy , $relative_file_name )  =  split( /\Q$root_folder\E\// , $i_file_name , 2 );
    push  ( @{ $io_hash_ref->{_generateIndex($i_file_name)} }  , $relative_file_name );

   $logger->Info( "File [$relative_file_name] was added to hash");
  }
  else
  {
    $logger->Info( "File $i_file_name was skipped due to ignore list [" . join(',',@{$ignoreList_ref}) . "]"); 
  }
    
  return 1;
}
#########################################################################################################
sub _generateIndex($)
{
  my $file = shift;
  
  open ( FILE, "<$file") or $logger->Fatal("Can't open [$file], $!");
  binmode(FILE); 
  
  my $filesize = -s "$file";
  
  if ( 0 == $filesize )
  {
    $logger->Debug("File [$file] is empty");
    return 1;
  }
  
  #( int ( $filesize / $devider ) < $key_size / $devider ) ? int ( $filesize / $devider ) : $key_size / $devider
  
  #  each element is of 32 byte size 512 / 16
  my $devider   = 16;
  my $offset    = int ( $filesize / $devider ) ; 
  my $key_size  = 512;
  my $key     = 0;
  my $block_size = $key_size / $devider;
  
  my $t_key;

  for ( $i = 0 ; $i < $devider ;$i++ )
  {
    #read the part size or the left file size
    #my $size  = ($key_size/$devider < ( $filesize - $i * $offset ) ? $key_size/$devider : $filesize - $i * $offset );
    seek( FILE , $i * $offset , 0 );
    #read( FILE ,$t_key , $size , $i * $offset );
    
    read( FILE , $t_key , $block_size);
    
    $key .= $t_key;
  }
  
  close (FILE);
  
  my $index =  md5_hex($key);
  
  $logger->Info( "File $file index [$index]");
  
  #convert it to 128 bit < 512 byte key to 16 byte key> 
  return $index;
}
#########################################################################################################
sub _buildIndex_HDFS( $$;$ )
{
   my  ( $dir_from , $hash , $ignoreList ) = @_;
   
   unlink ( $logdir. "/index.hash" ) if ( -e $logdir. "/index.hash" );  
   
   #add support to first time execution , file does not exists
   # read file from HDFS
   # fill the hash
   if ( AbstractHandler::executeCommand( $logger, 'hdfs dfs -ls "' . hdfs_escape("$dir_from/index.hash") . '" > /dev/null 2>&1') )
   {
     my $hdfs_command = 'hdfs dfs -copyToLocal "' .  hdfs_escape("$dir_from/index.hash") . '" "' .  $logdir . '" > /dev/null 2>&1';
     AbstractHandler::executeCommand($logger,$hdfs_command) or return $logger->Error("Failed to load index file to local FS from [$dir_from] $!");
   
     open ( HASH_INDEX_FILE , "<".$logdir . "/index.hash" ) or return $logger->Error( "Failed to open file [" . $logdir . "/index.hash ]" ); 
     %{ $hash } = %{ eval ( <HASH_INDEX_FILE> ) };
     close HASH_INDEX_FILE;
    
     unlink ( $logdir . "/index.hash" );  
   }
    
   return 1;
}
#########################################################################################################
sub _buildIndex_FS( $$;$ )
{
  my  ( $dir_from , $hash , $ignoreList ) = @_;
  
  $logger->Info( "Start to build index for [$dir_from]");
  
  #return success if the folder does not exists
  #it will be created later on during copy stage 
  #or will not be created in case nnothing to copy
  return 1 if (! -d $dir_from ) ;
  
  #anonymuse array get created with [] if handler needs more params
  #need to pass it to _buidIndex and add to []    
    AbstractHandler::scanPath( $logger, $dir_from , \&_handler , [ $hash , $ignoreList , $dir_from ] ) or return $logger->Error( "Failed to build index for [$dir_from]");
  
  #print keys %{$hash} ,"\n";
  
  return 1;
}
#########################################################################################################
sub _sync_HDFS ( $$$$ )
{
   my %source_hash = %{$_[0]};
   my %dest_hash = %{$_[1]};
   my $dir_from = $_[2];
   my $dir_to = $_[3]; 
   my $hdfs_command;
   my %files_to_sync;
   
   #during syncing the anonymos function will be called that will populate the hash
   _sync ( \%source_hash , \%dest_hash , $dir_from , $dir_to , sub { push ( @{ $files_to_sync{ $_[1] } }  , $_[0] ) } ) or return $logger->Error ( "Failed to sync $dir_from -> $dir_to " );
     
   $logger->Info("Identified " . scalar( keys ( %files_to_sync ) ) . " different files , starting syncronization" );
    
   foreach $hdfs_path ( keys %files_to_sync )
   {
      $hdfs_command = 'hdfs dfs -mkdir -p "' . hdfs_escape($hdfs_path) . '" >/dev/null 2>&1';
      #$loggerSTDOUT->Debug( $hdfs_command );
      AbstractHandler::executeCommand( $logger, $hdfs_command );# or return $logger->Error("Failed to create a directory in hadoop [$hdfs_path] $!");
      
      #make a copy in bulks as there is a limit on a command line size
      my $bulk_size = 128;
      my $index_to  = $#{ $files_to_sync{ $hdfs_path } } ;
      my $index_from = 0;
      
      do
      {
        $index_from =  ( $index_to > $bulk_size ) ? (  $index_to - $bulk_size + 1 ) : 0 ;
         
        $logger->Debug( "Copying indexes [$index_from - $index_to] total list size " . scalar @{ $files_to_sync{ $hdfs_path } } . " bulk size $bulk_size");
 
        $hdfs_command = 'hdfs dfs -put -f ' . join ( " " , map ( uri_escape( $_ )  , @{$files_to_sync{ $hdfs_path }}[ $index_from .. $index_to ] ) ) . ' "'. hdfs_escape($hdfs_path) . '" >/dev/null 2>&1';
        
        $logger->Debug( $hdfs_command );
        
        AbstractHandler::executeCommand( $logger, $hdfs_command ) or return $logger->Error("Failed to copy to hdfs folder  [$hdfs_path] $!");    
        
        $index_to = $index_from - 1;
      
      } while ( $index_from > 0 );
   }
   
   logger->Info("Copy to HDFS has finished , syncing the hash files");
   
   #update the hash by copied files information
   #i know it is less efficient as it going over the list of files in second time
   # but the list assumed to be small , as the chanegs are incremental , and this is more clear solution from code perspective
   foreach $hdfs_path ( keys %files_to_sync )
   {
       foreach ( @{$files_to_sync{ $hdfs_path }} )
       { 
          _handler ( $_, [ \%dest_hash , $ignoreList , $dir_from ]);   
       }
   }
 
   $logger->Info("Copy to HDFS has finished , puting a new index file");
   open ( FH , ">". $logdir . "/index.hash"  ) or return $logger->Error("Failed to create index file for HDFS $!");
   print FH Dumper( \%dest_hash );
   close FH;
   
   #delete the previuse file , will not fail if file not exists
   $hdfs_command = 'hdfs dfs -rm -f "' . hdfs_escape("$dir_to/index.hash")  . '" > /dev/null 2>&1';
   AbstractHandler::executeCommand( $logger, $hdfs_command );
   
   #put a new version of the file
   $hdfs_command = "hdfs dfs -put " . uri_escape( "$logdir/index.hash" ) . ' "' . $dir_to . '" > /dev/null 2>&1' ;
   AbstractHandler::executeCommand( $logger, hdfs_escape($hdfs_command) ) or return $logger->Error("Failed to upload index file to HDFS $!");
   
   #delete the local file
   unlink ( $logdir . "/index.hash" );
   
   return 1;  
}
#########################################################################################################
sub _sync_FS( $$$$ )
{
    return _sync( $_[0] , $_[1] , $_[2] , $_[3] , \&_copy );
}
#########################################################################################################
sub _sync ( $$$$$;$ )
{
    my %source_hash = %{$_[0]};
    my %dest_hash = %{$_[1]};
    my $dir_from = $_[2];
    my $dir_to = $_[3];
    my $_sync_operation = $_[4];
    my $_sync_operation_params = $_[5] if defined $_[5];
     
    #need to add handling of source and dist names
    foreach $s_key ( keys %source_hash )
    {
        if ( ! exists $dest_hash{$s_key} )
        {
            foreach $filename ( @{$source_hash{$s_key}} )
            {
                #look for relative path of file according to start sync dir     
                my $relative_path = dirname( $filename );#$' if dirname($filename) =~ /\Q$dir_from\E/;

                if ( _validate( $dir_from ."/". $filename ) )
                {
                    $logger->Debug( "Syncing $dir_from/$filename -> $dir_to/$filename");
                    &$_sync_operation( $dir_from ."/". $filename , $dir_to ."/". $relative_path , $_sync_operation_params ) or $logger->Error("Failed to sync $dir_from/$filename to $dir_to/$relative_path $!"); 
                }
                else
                {
                   $logger->Debug( "Skiping $dir_from/$filename -> $dir_to/$filename due to valdation"); 
                }
             }
        }
        else #file does exist
        {
            #check if the same path
            
            foreach $filename ( @{$source_hash{$s_key}} )
            {
                my $relative_path_from = dirname( $filename );#$' if dirname($filename) =~ /\Q$dir_from\E/;
            
                my $found = undef;

                #find the relative path in dest_hash
                foreach ( @{$dest_hash{$s_key}} )
                {
                    my $relative_path_to = dirname( $_ );#$' if dirname($_) =~ /\Q$dir_to\E/;
                       
                    if ( $relative_path_to eq $relative_path_from )
                    {    
                        $logger->Debug ( "Checking the following paths equvalent for [$_] [$relative_path_from] -> [$relative_path_to] - True" );
               
                        $found = 1;
                        last;
                    }
                    else
                    {
                         $logger->Debug ( "Checking the following paths equvalent for [$_] [$relative_path_from] -> [$relative_path_to] - False" );
                    }
                }
        
                #not found such path need to sync       
                if ( ! $found )
                {
                    $logger->Debug( "File exists , but in different folder , need to sync $dir_from/$filename -> $dir_to/$relative_path_from");
                    
                    if ( _validate ( $dir_from ."/". $filename ) )
                    {
                      &$_sync_operation( $dir_from ."/". $filename , $dir_to ."/". $relative_path_from , $_sync_operation_params ) or $logger->Error("Failed to sync $dir_from/$filename to $dir_to/$filename $!");    
                    }
                    else
                    {
                       $logger->Debug( "Skiping $dir_from/$filename -> $dir_to/$filename due to valdation"); 
                    }
                 }
            }
        } #else
    } 
    
    return 1;   
}
##############################################################################
sub _copy ( $$ )
{
  my ( $from , $to ) = @_;

  my $origin_stats = stat($from);

    $loggerSTDOUT->Info( "Copy $from , $to size :". AbstractHandler::convertSizeToPrintable( $logger, $origin_stats->size ) ) ; 
      
    #create the folder as a full path 
    mkpath( $to ) if ( ! -e $to);
    #phisical copy to a temp file
    copy( $from , $to . "/_temp_" ) or return $logger->Error("Failed to copy $from to $to $!"); 
      
    #check the temp file size and the original file are the same, better to make cksum,  but this is ok as well
    my $dest_stats = stat( $to . "/_temp_" );
      
    if ( $origin_stats->size == $dest_stats->size )
    {
       move ( $to . "/_temp_" , $to . "/" . basename( $from ) ) or return $logger->Error("Failed to move " . $to . " _temp " . " to $to $!"); 
       return 1;
    }
    else
    {
       unlink ( $to . "/_temp_" );
       return $logger->Error( "Failed to copy $from to  $to , the size is different after the copy : origin size [" . $origin_stats->size . "] dest size [" . $dest_stats->size ."]");
    }
}
#########################################################################################################
sub print_stats
{ 
    print ( '.' );
    alarm 1;
}
#########################################################################################################
sub main()
{   
  my @source_array;
  my $source = undef;
  my $dest = undef;
  my @ignore;
  
  my $begine = undef;
  my $delta = undef;
  
  GetOptions(
     "s=s{1,}"=>\@source_array,
     "d=s{1}"=>\$dest,
     "e=s"=>\@ignore,
     "v"=>sub{$logger = $loggerSTDOUT;},
     "h"=>sub{print "$USAGE\n";exit 0;});
  
  if ( ! defined $source_array[0]  || !defined $dest)
  {
    return $loggerSTDOUT->Fatal("One of mandatory input parameter is missig $USAGE");
  }
  
    #fill in exceptions allow to have multiple -e and also to have -e 3,4,56
  @ignore = split (/,/ , join (',' , @ignore));

  $SIG{ALRM} = \&print_stats;
  #regular FS
  if ( $dest !~ /^hdfs:/ )
  {
      $loggerSTDOUT->Debug("The target is a standart FS");
      $_buildIndex = \&_buildIndex_FS;
      $_sync = \&_sync_FS;
  }
  else#HDFS FS
  {
      $dest =~ s/^hdfs://m;
      
      $loggerSTDOUT->Debug("The target is a HADOOP FS");
      $_buildIndex = \&_buildIndex_HDFS;
      $_sync = \&_sync_HDFS;
  }
    
  $loggerSTDOUT->Debug( "Start Indexing destination dir [$dest]" ) ;
  $logger->Info( "Start Indexing destination dir [$dest]" ) ; 
    
  #build dest hash
  my %hash2;

  foreach $source ( @source_array )
  {
    if ( $source =~ /^hdfs:/ )
    {
         $loggerSTDOUT->Error( "Wrong defenition of source , HDFS could be only the target file system");
         next;
    }
    
    &$_buildIndex( $dest, \%hash2 ) or return $loggerSTDOUT->Error("Failed to build index for [$dest]");

    $loggerSTDOUT->Debug( "Finish Indexing , destination dir [$dest]") ;    
    
    $loggerSTDOUT->Debug( "Start Indexing source dir [$source] " . ( ( @ignore > 0) ? " , ignore list :" . join (',' , @ignore) : "" ) ) ;
    
    $logger->Info( "Start Indexing source dir [$source]" ) ;
        
    #build source hash with ignore list if provided
    my %hash1;
    _buildIndex_FS( $source, \%hash1 , \@ignore ) or return $loggerSTDOUT->Error("failed to build index for [$source]");
        
    $loggerSTDOUT->Debug( "Finish Indexing , source dir [$source]") ; 
    
    #   DEBUG hash do not delete
    #   foreach $val (values %hash1 )
    #   {   
    #       foreach (@{$val})
    #       {
    #           my ( $vol, $dir, $file )  = File::Spec->splitpath( $_ );
    #           print $dir . " -> " , $file;
    #           
    #           my @dirs = File::Spec->splitdir( $dir );
    #           print map qq[ "$_" ], @dirs;
    #       }
    #       print "\n";
    #   }
        
    $logger->Info( "Finish Indexing , source hash size " . keys( %hash1 ) . " dest hash size " . keys( %hash2 )) ;  

   # print "Source:\n";
   # print $_."\n" for keys ( %hash1 );
   # print "Source:\n";
 
   # print "Destination:\n";
   # print $_."\n" for keys ( %hash2 );
   # print "Destination:\n";
        
   $loggerSTDOUT->Info( "Start sync [$source -> $dest]" ) ;   
        
   $begin = time(); 
        
   #sync files form hash1 to hash2 by coping missing files
   &$_sync( \%hash1 ,\%hash2, $source , $dest ) or return $loggerSTDOUT->Error ( "Failed to perform sync" );
        
   $delta += time() - $begin;
        
   $loggerSTDOUT->Info( "Finish sync [$source -> $dest]" ) ;
    
  }#foreach 
    
  $logger->Info( "Finish sync, $gCopyCounter files copied , transfered " . AbstractHandler::convertSizeToPrintable($logger , $gTotalVolume) . ( $delta > 0 ? " , average speed :  " . AbstractHandler::convertSizeToPrintable($logger , $gTotalVolume/$delta) . "/s" : "" ) );  
}

########
##MAIN##
########
main();

