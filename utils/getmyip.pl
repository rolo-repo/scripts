#!/usr/local/bin/perl
BEGIN 
{
	BEGIN
	{
		push @INC, "C:/PERLMODULES/","../PERLMODULES/", $ENV{PERLMODULES},"$ENV{HOME}//PERLMODULES/";
	}
	
	use AbstractHandler;
	use Logger::Logger;
	use Cwd;
	use File::Basename;
	
	$logdir = defined $ENV{APPL_LOG} ?  $ENV{APPL_LOG} : getcwd() . "/log/";
	$gCurrent_date = AbstractHandler::getCurrentDate();
	$gCurrentTime = AbstractHandler::getCurrentTime();
	
	$logger       = new Logger( TRACE , basename($0,'.pl') . "_".$gCurrent_date."_" . $gCurrentTime . ".log" , $logdir);
	$loggerSTDOUT = new Logger(DEBUG);
	
	$USAGE="USAGE: $0 ( -e , -l )\n\t-e external ip\n\t-l local ip";
}

use Getopt::Long;
use LWP::Simple;
use IO::Socket;
use IO::Socket::INET;


sub getExternalIP()
{
	#<html><head><title>Current IP Check</title></head><body>Current IP Address: 84.228.128.117</body></html>
	my $content = get ('http://checkip.dyndns.org');
	$content =~ /Address:\s*(.*?)</;
	
	#print $1;
	return $1
}

sub getInternalIP()
{
	my $sock = IO::Socket::INET->new(
                       PeerAddr=> "192.168.1.1",
                       PeerPort=> 80,
                       Proto   => "tcp");

	my $ip = $sock->sockhost;
	
	return $ip;
}

##########
###MAIN###
##########


GetOptions(
  	 "e"=>sub{ my $ip = getExternalIP() ; print $ip ; exit 0;},
   	 "l"=>sub{ my $ip = getInternalIP() ; print $ip ; exit 0;});

$loggerSTDOUT->Fatal("One of mandatory input parameter is missig\n$USAGE");