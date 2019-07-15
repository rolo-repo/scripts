package AbstractHandler;

#Description: AbstractHandler.pm
#This module responsable for all general methods for operation handlers
#All it methods are extracted to namespace of the caller
use File::Path;
use File::Basename;

require Exporter;
@ISA    = qw(Exporter);
@EXPORT =
  qw( executeCommand scanDir getDigit convertSizeToPrintable getCurrentTime getCurrentDate isDirectory readdir translate_permition_mode mkdir populateEnvironmentVariables limitedReadDir);

#constants
my %permitions = ( 'w' => 2, 'r' => 4, 'x' => 1 );


# -----------------------------------------------------------------
# Description  : Returns the current date in formatt yyymmdd
# Recives      : 1st - delimeter [optional]
# Returns      : date in format yyyymmdd or with any delimiter yyyy-mm-dd
# -----------------------------------------------------------------
sub getCurrentDate(;$) {
 my ( $gSec, $gMin, $gHour, $gDay, $gMon, $gYear ) = localtime(time);
 $gYear += 1900;
 $gMon++;
 $gMon = '0' . $gMon unless length($gMon) > 1;
 $gDay = '0' . $gDay unless length($gDay) > 1;

 return $gYear . $_[0] . $gMon . $_[0] . $gDay if defined $_[0] ;
 return $gYear . $gMon . $gDay;
}

# -----------------------------------------------------------------
# Description  : Returns the current time in format HH24MISS
# Recives      : null
# Returns      : date in format HH24MISS
# -----------------------------------------------------------------
sub getCurrentTime() {
 my ( $gSec, $gMin, $gHour, $gDay, $gMon, $gYear ) = localtime(time);

 $gHour = '0' . $gHour unless length($gHour) > 1;
 $gMin  = '0' . $gMin  unless length($gMin) > 1;
 $gSec  = '0' . $gSec  unless length($gSec) > 1;
 return $gHour . $gMin . $gSec;
}

# -----------------------------------------------------------------
# Description  : Check if requested environment variable exist if not terminating the program
# Recives      : name of environment variable , optional place for error buffer if not provided the process fails
# Returns      : value of environment variable
# -----------------------------------------------------------------
sub _checkEnvironmentVariable($;$) 
{
    ( defined $ENV{$_[0]} || $_[0] eq 'SUB_VERSION' ) 
      ? return $ENV{$_[0]} 
      : (( defined $_[1] ) 
         ?  ( ${$_[1]} = "environment variable [$_[0]] not defined" )
         : die "requested environment variable [$_[0]] not defined\n" );
    
}

# -----------------------------------------------------------------
# Description  : replace all occurrences of environment variable by it value
#      		     Environment variable should be placed in pattern $ENV_VAR_NAME
# Recives      : references to string contains env. variables
# Returns      : '0' or error description
# -----------------------------------------------------------------
sub populateEnvironmentVariables
{
 #   $$_ =~ s/\$(\w+)/&_checkEnvironmentVariable($1)/eg for @_;
 my $rc = '0' ;
 foreach ( @_ )
 {
  # *nix pattern
  $$_ =~ s/\$\{?(\w+)}?/&_checkEnvironmentVariable($1,\$rc)/eg;
  
  # Win pattern %XXX%
  $$_ =~ s/\%\(?(\w+)\)?%/&_checkEnvironmentVariable($1,\$rc)/eg;
 }

 return $rc;
}

# -----------------------------------------------------------------
# Description  : check for existent of some folder
# Recives      : path to the folder
# Returns      : TRUE/FALSE
# -----------------------------------------------------------------
sub isDirectory($) {
 ( !-d $_[0] ) ? return 0 : return 1;
}

# -----------------------------------------------------------------
# Description  : read content of folder by specified pattern
# Recives      : 1st - direcory to read from
#                2nd - file pattern to search
# Returns      : reference to array of file names according to pattern
# -----------------------------------------------------------------
sub readdir($$) 
{
 my ( $dir_from, $file_pattern ) = @_;

 opendir( DIR, $dir_from );
 my @full_dir_content = readdir(DIR);
 my @dir_content;

 #if ($file_pattern eq '*'){
 #	push @full_dir_content ,<$dir_from/.*> ;
 #	}

 my $file;
 foreach $file (@full_dir_content) {
  $file = basename($file);

  next if ( $file eq '.' || $file eq '..' );

  push @dir_content, $file;
 }
 
 close(DIR);
 
 return \@dir_content;
}

# -----------------------------------------------------------------
# Description  : read content of folder by specified pattern limit the pattern to (* or ?)
#				or if pattern not specified return the file name
# Recives      : 1st - logger
#                2nd - source directory
#			   	 3td - file pattern to search (*,?)
# Returns      : reference to array of file names
# -----------------------------------------------------------------
sub limitedReadDir($$$) {
 my ( $logger, $dir_from, $file_pattern ) = @_;
 $logger->Trace("Check for mettacharacters [$dir_from/$file_pattern].");
 if ( $file_pattern =~ /\*|\?/ ) {
  $logger->Trace("Mettacharacter found in [$dir_from/$file_pattern].");
  return AbstractHandler::readdir( $dir_from, $file_pattern );
 }
 else {
  $logger->Trace("Mettacharacter was not found in [$dir_from/$file_pattern].");
  ( -e "$dir_from/$file_pattern" ) ? return [$file_pattern] : return [];
 }
}

# -----------------------------------------------------------------
# Description  : translate permition mode from characters to octal form
# Recives      : 1st - mode in characters (w - write r - read x - exec)
# Returns      : mode in octal form.
# -----------------------------------------------------------------
sub translate_permition_mode($) {
 my ($mode) = @_;

 my $temp;
 foreach ( keys %permitions ) {
  $temp += $permitions{$_} if ( $mode =~ /$_/ );
 }

 return oct( '0' . ($temp) x 3 );
}

# -----------------------------------------------------------------
# Description  : create directory or whole path
# Recives      : 1st - logger to use
#                2nd - new directory/es path
#				 3rd - mode of new directory.[optional]
# Returns      : TRUE/FALSE.
# -----------------------------------------------------------------
sub mkdir($$;$) {
 my ( $logger, $path, $mode ) = @_;
 $mode = 'wrx' if ( !defined $mode );
 $logger->Trace("Creat directory Path [$path] Mode [$mode]]");
 if ( !isDirectory($path) ) {
  eval { mkpath( $path, 0, translate_permition_mode($mode) ) };
  return $logger->Error("Can't create directory [$path] [mode]") if ($@);

 }
 else {
  $logger->Trace("[$path] already exist.");
 }
 $logger->Trace("Exit mkdir");
 return 1;
}

# -----------------------------------------------------------------
# Description  : Retrieves any digit number form input numeric value
# Recives      : 1st - logger
#                2nd - requested digit
#			   	 3td - input number
# Returns      : desired digit or 0 if out of bound
# -----------------------------------------------------------------
sub getDigit( $$$ ) 
{
 my ( $logger, $desired_digit, $i_number ) = @_;

 my @digits = undef;

 do { unshift @digits, ( $i_number % 10 ); $i_number = int( $i_number / 10 ); }
   while ( $i_number != 0 );

 return ( $desired_digit > @digits ) ? 0 : $digits[ $desired_digit - 1 ];
}

# -----------------------------------------------------------------
# Description  : Convet number representing volume to printable format XXX.XX GB or XXX.XX MB
# Recives      : 1st - logger
#                2nd - input volume
#			   	 3td - requested precision - optional , 2 is default
# Returns      : volume in format XXX.XXX GB/MB/Kb/B
# -----------------------------------------------------------------
sub convertSizeToPrintable ($$;$)
{

 my ( $logger, $i_value, $i_precision ) = @_;

 #default value
 $i_precision = 2 if ( !defined $i_precision );

 my $val = 0;
 my $result;

 if ( $i_value > ( 1 << 30 ) ) {
  $val = $i_value >> 30;

  $result .= $val;
  $result .= '.';

  $result .= AbstractHandler::getDigit( $logger, $i_precision-- , $i_value - $val * ( 1 << 30 ) )
    for ( $i_precision > 0 );

  #$result .= _getDigit(2, $i_value - $val * ( 1 << 30 ) );
  $result .= " GB";
 }
 elsif ( $i_value > ( 1 << 20 ) ) {
  $val = $i_value >> 20;

  $result .= $val;
  $result .= '.';
  $result .= AbstractHandler::getDigit( $logger, $i_precision-- , $i_value - $val * ( 1 << 20 ) )
    for ( $i_precision > 0 );
  $result .= " MB";
 }
 elsif ( $i_value > ( 1 << 10 ) ) {
  $val = $i_value >> 10;

  $result .= $val;
  $result .= '.';
  $result .= AbstractHandler::getDigit( $logger, $i_precision-- , $i_value - $val * ( 1 << 10 ) )
    for ( $i_precision > 0 );
  $result .= " Kb";
 }
 else {
  $result .= $i_value;
  $result .= " B";
 }

 return $result;
}


# -----------------------------------------------------------------
# Description  : scan dir starting from given point and execute handler on each file
# Recives      : 1st - logger
#                2nd - start scan full path
#			   	 3td - handler method
#                4th - handler params
# Returns      : 1 Success , undef error
# -----------------------------------------------------------------
sub scanPath($$;$$)
{
	my ( $logger , $dir_from ,$handler, $handler_param ) = @_;

	if (! -d $dir_from ) 
	{
		return $logger->Error("Directory [$dir_from] does not exist ,scan failed");
	}
	
	#prepare default list of file by reading content of directory
	
	my $r_file_from = AbstractHandler::readdir( $dir_from, "*" );
	
	#unreference pointers
	my @file_from_list = @$r_file_from;
	
	#loop throw all files in the source list
	for ( my $i = 0 ; $i < @file_from_list ; $i++ ) 
	{
		my $file_from = $file_from_list[$i];

		#If source is directory , enter inside and call recursively.
		if ( -d "$dir_from/$file_from" ) 
		{
			$logger->Debug(
				"Subdirectory found [$dir_from/$file_from] , entering");

			#recursive call to it self
			AbstractHandler::scanPath( $logger, "$dir_from/$file_from", $handler ,$handler_param )
			  or return $logger->Error("Seek content of subdirectory [$dir_from/$file_from] failed");
		}
		else
		{
			$logger->Debug("Handling [$dir_from/$file_from]");

			 &$handler( "$dir_from/$file_from", $handler_param )
			  or return $logger->Error("Failed to handle [$dir_from/$file_from], $!");
		}
	}

	return 1;#sucees error is undef
}


# -----------------------------------------------------------------
# Description  : executes a command and checking return code
# Recives      : 1st - logger
#                2nd - command
#                3td - handler method
#                4th - handler params
# Returns      : 1 Success , undef error
# -----------------------------------------------------------------
sub executeCommand($$;$$) 
{
	my ($logger,$command,$handler, $handler_param) = @_;

    $logger->Trace("Execute [$command]");
   
    if ( defined $handler )
    {
     open( COMMAND, $command.' 2>&1 |' ) or 
      return $logger->Error("Execution of  [$command] failed with code : $!");
   
     while ( <COMMAND> )
     {
        &$handler( $_ , $handler_param ) 
         or return $logger->Error("Failed to handle [$_], $!");
     }
     
     close( COMMAND );
     
     return 1;
    }
    
	my $ret = system($command);
	#$exit_value  = $? >> 8;
   # $signal_num  = $? & 127;
   # $dumped_core = $? & 128;
	if (0 != $ret)
	{
	  if ( ( $code =  $ret >> 8 ) != 0 ) 
      {
        return $logger->Error(
            "Execution of  [$command] failed with code : [$code],$!");
      }
    
      if ( ( $signal_num =  $ret & 127 ) != 0 ) 
      {
         return $logger->Error(
            "Execution of  [$command] failed with signal :  [$signal_num],$!");
      }
    
      if ( ( $dumped_core = $ret & 128 ) != 0 ) 
      {
          return $logger->Error(
            "Execution of  [$command] failed with core : dumped_core [$dumped_core],$!");
      }  
      
      return $logger->Error(
            "Execution of  [$command] failed result [$ret] , uknown reason,$!");
	}
	
    
	return 1;
}

1;
