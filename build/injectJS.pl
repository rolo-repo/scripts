#!/usr/local/bin/perl

BEGIN {

  BEGIN
  {
    push @INC, ".", "C:/PERLMODULES/","../PERLMODULES/", $ENV{PERLMODULES},"$ENV{HOME}//PERLMODULES/";
  }
  use AbstractHandler;
  use Logger::Logger;
  use File::Basename;
  use File::Copy;
  
  $logger = new Logger( DEBUG );
  $logger->Info( "Execute : " . $0 );
  
  $exe =  basename($0);
}

my ( $resource , $source ) = @ARGV;

sub _getJS($)
{
	  my $content = undef; 
	  
	  open( my $fh, '<', $source ."/". $_[0] . ".js") or return "";
   # {
    #    local $/;#make the file read fully till the end
   #     $content = <$fh>;
   # }
   
   my $multilinecomment = undef;
   
   while(<$fh>) {
   	$_ =~ s/\/\/.*$//;
	  next if /^\s*$/;
	  $_ =~ s/^\s*//;
	  
	  $multilinecomment  = 1 if ( /\/\*/);
	  $multilinecomment  = undef if ( /\*\//);
	  
	  next if /^\s*\/\*$|\*\//;  
	  next if $multilinecomment;
	  
	 
  	$content .= $_;
	}

  close($fh);
    
  #$logger->Info($content);
  
  return $content;
}

open(INPUT,"<". $resource ) or $logger->Fatal("Failed to open " . $resource );
open(TMP,">" . $resource . "_tmp") or $logger->Fatal("Failed to open " . $resource . "_tmp" );

while(<INPUT>)
{
 $_ =~ s/\$\{?script_(\w+)}?/&_getJS($1)/eg;
 $_ =~ s/^\s*//;
 print TMP  $_;
}

close(INPUT);
close(TMP);

move( $resource . "_tmp", $resource ) or $logger->Fatal("Failed to create " . $resource );

END
{
	 $logger->Info( $exe . "- Done" );
}

