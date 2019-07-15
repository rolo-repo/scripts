#!/usr/local/bin/perl
# The application scans the range of IPs and identifies host names and MAC addresses 
# the scripts assumes arping applcaition is available to be executed
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
    $loggerSTDOUT = new Logger(INFO);
    
    $USAGE="USAGE: $0 --from <from> --to <to> ( eg --from 10.234.8.233 --to 10.234.8.235 )";
}



use Socket;
use Net::Ping;
use Getopt::Long;
use AbstractHandler;

$| = 1;

sub scan($$)
{
    $loggerSTDOUT->Info("Scaning from " . $_[0] . " to " . $_[1] );
    my $start_ip = inet_aton($_[0]);
    my $end_ip = inet_aton($_[1]);
    
    my %active;
    my @active_ips;
    
    for ( my $ip = unpack("N",$start_ip) ; $ip <= unpack("N",$end_ip) ; $ip++ )
    {
       my $l_bynaryIP = pack("N", $ip );
   
       $p = Net::Ping->new("udp", 0.5 );
    #   $name  = gethostbyaddr( $l_bynaryIP, AF_INET);
       if ( $p->ping(inet_ntoa( $l_bynaryIP )))
       { 
       		 push @active_ips , $l_bynaryIP;
           $loggerSTDOUT->Info( "IP: [" . inet_ntoa( $l_bynaryIP ) . "] found");   
       }
   }
   
   foreach (@active_ips)
   {
   		 my $name  = gethostbyaddr( $_, AF_INET);
       if ( defined $name )
       { 
           $active{$_} = inet_ntoa( $_ );
           $loggerSTDOUT->Info( "IP: [" . $active{$_} . "] Host name : [" . $name . "]");   
       }
   }
   
  
   foreach (values %active)
   {
       AbstractHandler::executeCommand( $loggerSTDOUT, 'arping -I eth0 -c1 ' . $_ . ' | grep Unicast' , 
                sub { $loggerSTDOUT->Info("HOST :" . $_[1] . "INFO :" . $_[0] ) } , $active{$_} );
      
   } 
   
   $loggerSTDOUT->Error( "Nothing was found in range of IPs" . $_[0] . " to " . $_[1]) if ((keys %active) == 0);   
}
sub main()
{
    GetOptions(
     "from=s"=>\$from,
     "to=s"=>\$to,
     "h"=>sub{print "$USAGE\n";exit 0;});
     
    scan($from,$to);
}

main();
