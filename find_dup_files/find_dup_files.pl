#!/usr/local/bin/perl
BEGIN 
{
	BEGIN
	{
		push @INC, "C:/PERLMODULES/","../PERLMODULES/" , $ENV{PERLMODULES},"$ENV{HOME}//PERLMODULES/";
	}
	use AbstractHandler;
	use Logger::Logger;
	use Cwd;
	$logdir = defined $ENV{APPL_LOG} ?  $ENV{APPL_LOG} : getcwd()."/log/";
	$gCurrent_date = AbstractHandler::getCurrentDate();
	$gCurrentTime = AbstractHandler::getCurrentTime();
	$logger       = new Logger( INFO, "dup_file_".$gCurrent_date."_".$gCurrentTime.".log" , $logdir);
	$loggerSTDOUT = new Logger(TRACE);
}


use File::Copy;
use File::stat;
use File::Basename;
use Logger::Logger;
use Digest::MD5 qw(md5_hex);

my %gHash;
my $scanned = 0;
my $suspect_duplicate = 0;

sub _seek_dir($;$) {
	my ( $dir_from, $r_file_from ) = @_;

	#create destination directory if it does not exist
	if (! -d $dir_from ) 
	{
		$logger->Error("[$dir_from] not exist copy failed skipped");
		return 1;
	}
	
	#prepare default list of file by reading content of directory
	if ( !defined $r_file_from ) 
	{
		$r_file_from = AbstractHandler::readdir( $dir_from, "*" );
	}
	
	#unreference pointers
	my @file_from_list = @$r_file_from;
	
	#loop throw all files in the source list
	for ( my $i = 0 ; $i < @file_from_list ; $i++ ) {
		my $file_from = $file_from_list[$i];

		#If source is directory , enter inside and call recursively.
		if ( -d "$dir_from/$file_from" ) 
		{
			$logger->Trace(
				"Subdirectory found [$dir_from/$file_from] , entering");

			#recursive call to it self
			_seek_dir( "$dir_from/$file_from", undef )
			  or return $logger->Error("Seek content of subdirectory [$dir_from/$file_from] failed");
		}
		else
		{
			#copy single file
			$logger->Trace("Handling [$dir_from/$file_from]");

			_handle_file( "$dir_from/$file_from" )
			  or return $logger->Error("Failed to handle [$dir_from/$file_from], $!");
		}
	}
	return 1;
}

sub _handle_file($)
{
	my $file = shift;
	print ".";
	
	$scanned++;
	
	open ( FILE, "<$file") || $logger->Fatal("Can't open $file, $!");
	binmode(FILE); 
	
	my $filesize = -s "$file";
	
	if ( 0 == $filesize)
	{
		$logger->Debug("File $file is empty");
		return 1;
	}
	
	#( int ( $filesize / $devider ) < $key_size / $devider ) ? int ( $filesize / $devider ) : $key_size / $devider
	
	#  each element is of 32 byte size 512 / 16
	my $devider 	= 16;
	my $offset 		= int ( $filesize / $devider ) ; 
	my $key_size 	= 512;
	my $key 			= 0;
	my $block_size = $key_size/$devider;
	
	my $t_key;
	#$loggerSTDOUT->Info($file);
	
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
	
	#convert it to 128 bit < 512 byte key to 16 byte key>
	$key = md5_hex($key);
	
	if ( ! exists $gHash{$key} )
	{
		$gHash{$key} = $file;
	}
	else
	{
		$suspect_duplicate++;
		$logger->Info("File $file is duplicated to ".$gHash{$key});
	}
		
	return 1;
}


##################
### MAIN #########

( $#ARGV < 0 ) ? print "Provide start dir" : _seek_dir($ARGV[0]);


$logger->Info("Scanned $scanned files , duplicated $suspect_duplicate");
