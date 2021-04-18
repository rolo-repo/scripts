#!/usr/local/bin/perl

BEGIN {

  BEGIN
  {
    push @INC, ".", "C:/PERLMODULES/","../PERLMODULES/", $ENV{PERLMODULES},"$ENV{HOME}//PERLMODULES/";
  }
  use AbstractHandler;
  use Logger::Logger;
  use File::stat;
  use File::Basename;
  
  $logger = new Logger( DEBUG );
  $logger->Info( "Execute : " . $0 );
  
  $exe =  basename($0);
}

my ($source , $targetDir) = @ARGV;

my $sb = stat($source);

$targetDir = ( defined $targetDir ) ? $targetDir : dirname($source);

$logger->Info( "$exe :target folder: " . $targetDir );  

open($OUT, ">".$targetDir."/__build_version.h");

print $OUT "#ifndef __build_version.h__". "\n";
print $OUT "#define __build_version.h__". "\n";
print $OUT "#define BUILD_ID " . $sb->mtime ."\n";
print $OUT "#endif//__build_version.h". "\n";

close $OUT;

END
{
  ( -e "$targetDir/__build_version.h" ) ? 
		$logger->Info( $exe . "- Done" ) :
		$logger->Fatal( "FAILED generation of " . "$targetDir/__build_version.h" . " failed!");
}