#!/usr/local/bin/perl
BEGIN
{

 BEGIN
 {
  push @INC, "C:/PERLMODULES/", "../PERLMODULES/", $ENV{PERLMODULES}, "$ENV{HOME}//PERLMODULES/";
 }

 use AbstractHandler;
 use Logger::Logger;
 use File::Basename;
 use Cwd;

 $logdir = defined $ENV{APPL_LOG} ? $ENV{APPL_LOG} : getcwd() . "/log/";

 $gCurrent_date = AbstractHandler::getCurrentDate();
 $gCurrentTime  = AbstractHandler::getCurrentTime();

 $logger = new Logger( TRACE, basename($0,'.pl')  . "_" . $gCurrent_date . "_" . $gCurrentTime . ".log",  $logdir );
 $loggerSTDOUT = new Logger(TRACE);
 
 $USAGE = "Usage -d <dir to start from> -p <list of patterns> -r ( for remove ) -a ( for archive ) . eg ( delete.pl -d . -p *.obj,*sln* -p .pdb -r -a)";
}

use File::Copy;
use File::stat;
use File::Basename;
use Logger::Logger;
use Getopt::Long;

$| = 1;

my $totalDeletedFileSize = 0;
my $totalDeletedFiles    = 0;


sub _fileInUse ( $ )
{
  my $rc = `lsof $_[0]`;
  return 1 if ( length ( $rc ) > 0 );
    
  return undef;    
}

###########################################################
sub _archive_handler ( $$ )
{
  my ( $i_file, $params ) = @_;
 
  my $pattern_ref = ${$params}[0];
  my $archive_period = ${$params}[1];
 
  my @pattern = @{$pattern_ref};
  
  my $full_pattern = quotemeta( $pattern[0] ) . '$';

  $full_pattern .= ( '|' . quotemeta($_) . '$' ) for @pattern[ 1 .. $#pattern ];

  #change \* to .*
#  $logger->Trace("1 full_pattern -> $full_pattern ");
  $full_pattern =~ s/\\\\\\\*/.*/g;
 # $logger->Trace("2 full_pattern -> $full_pattern ");
  
  #compile pattern
  $full_pattern = qr($full_pattern);

  my $mod_time = stat($i_file)->mtime;
 
  if ( ( basename( $i_file ) =~ /$full_pattern/ || $full_pattern eq '.*$' ) && ! _fileInUse ( $i_file ) && $mod_time < time() - $archive_period * 86400 )
  {
   AbstractHandler::executeCommand ( $logger , "gzip $i_file") or return $logger->Error ( "Failed to archive $i_file ") ;
   $totalDeletedFiles++;
  }
  
 return 1;
}

#################################################################
sub _delete_handler( $$ )
{
 my ( $i_file, $params ) = @_;

 #extract params
 my @pattern = @{$params};

 my $full_pattern = quotemeta( $pattern[0] ) . '$';

 $full_pattern .= ( '|' . quotemeta($_) . '$' ) for @pattern[ 1 .. $#pattern ];

  $full_pattern =~ s/\\\\\\\*/.*/g;
  
 #compile pattern
 $full_pattern = qr($full_pattern);
 
 if ( ( basename( $i_file ) =~ /$full_pattern/ || $full_pattern eq '.*$' ) && ! _fileInUse ( $i_file ) ) 
 {
  $logger->Info("Deleting File: $i_file");  $loggerSTDOUT->Trace("Deleting File: $i_file");

  my $fileSize = stat($i_file)->size;

  if ( unlink( $i_file ) )
  {
    $totalDeletedFileSize += $fileSize;
    $totalDeletedFiles++;
  }
  else
  { #error occured the calling methid will exit
    return $logger->Error("Failed to remove: $i_file $!");
  } #if
 }#if
 
 #success
 return 1;
}

##############################################################
sub _seek_dir_and_delete($;@)
{
 my ( $dir_from, @pattern ) = @_;

 AbstractHandler::scanPath( $logger, $dir_from, \&_delete_handler, \@pattern ) 
                                       or return $logger->Error("Failed in scan and delete");
 return 1;
}

##############################################################
sub _seek_dir_and_archive ($$$)
{
  my ( $dir_from, $pattern , $archive_period) = @_;
 
 AbstractHandler::scanPath( $logger, $dir_from, \&_archive_handler, [$pattern, $archive_period ] ) 
                                       or return $logger->Error("Failed in scan and archive");
 return 1;
}

##################
### MAIN #########

sub main()
{
 my %modes=('remove'=>1,'archive'=>2,'unused'=>8);
 
 my $path = undef;
 my $mode = undef;
 my @patterns;
 
 GetOptions(
  	 "d=s"=>\$path,
   	 "p=s"=>\@patterns,
   	 "r"=>sub{ $mode |= $modes{'remove'};},
   	 "a"=>sub{ $mode |= $modes{'archive'};},
   	 "h"=>sub{print "$USAGE\n";exit 0;});

  # -p can be p1,p2,p3 or -p p1 -p p2 ... use both as array
  @patterns = split (/,/ , join (',' , @patterns));
  
  #default mode = <remove>
  $mode = $modes{ 'remove' } if ! defined $mode;
  
  if ( ! defined $path || $#patterns < 0 )
  {
    $loggerSTDOUT->Fatal("One of required input parameters missing : $USAGE");
  }
  
  AbstractHandler::populateEnvironmentVariables(\$path);
  
  $loggerSTDOUT->Trace("Path : $path , Pattern :  @patterns ,  mode $mode");
   
   if ( $mode & $modes{'archive'} )
   {
   	_seek_dir_and_archive( $path,\@patterns , 4 ) or return $loggerSTDOUT->Fatal ( "Failed to perform archive action") ; 
   }
     
   if ( $mode & $modes{'remove'} )
   {
 	 _seek_dir_and_delete( $path, @patterns ) or return $loggerSTDOUT->Fatal ( "Failed to perform delete action") ; 
   }
   
  $loggerSTDOUT->Trace( "Total affected $totalDeletedFiles files , total size " .
                              AbstractHandler::convertSizeToPrintable( $logger, $totalDeletedFileSize ));
}

###################################################

main();
