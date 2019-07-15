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
 $loggerSTDOUT = new Logger(TRACE);
}

use IO::Socket::Multicast;

use constant GROUP => '224.0.55.55';
use constant PORT  => '2020';

my $sock = IO::Socket::Multicast->new(Proto=>'udp',LocalPort=>PORT ,Reuse=>1 );
$sock->mcast_add(GROUP) || $loggerSTDOUT>Fatal( "Couldn't set group: $!" );
$loggerSTDOUT->Info ( "Starting to wait " );

my $data;
my $size;
my %loggerHash;

while (1) 
{
   $sock->read( $size , 2);
   $loggerSTDOUT->Trace ( "Got size " . unpack ('s' , $size ));
   $size = unpack ( 's' , $size );
   if (  $size > 1024 )
   {
       $sock->mcast_drop(GROUP);
       sleep ( 10 ); 
       $sock->mcast_add(GROUP);
   }
  
   $loggerSTDOUT->Trace ( "Going to receve  " . $size . " bytes" );
   $sock->read( $data, $size );
   $loggerSTDOUT->Trace ( "Got " . $data . " " . length ($data));
   
   $data =~ /^(\w+)(.*)?$/;
   
   my $app = $1;
   if ( ! exists $loggerHash{$app} )
   {
     $loggerHash{$app} = { 'FH' => new IO::File(">>$logdir/$app"  . '_' . $gCurrent_date . '_' . $gCurrentTime . '.log') , 'SIZE' => 0 , 'INDEX' => 0 } ; 
   }
   else
   {
       if  ( $loggerHash{$app}->{'SIZE'} > ( 1024 * 1024 ) ) 
       {
          $loggerHash{ $app }->{'FH'}->close();
          $loggerHash{ $app }->{'FH'} = new IO::File(">>$logdir/$app" . '_' . $gCurrent_date . '_' . $gCurrentTime . '_' . $loggerHash{ $app }->{'INDEX'} . ".log");
          $loggerHash{ $app }->{'INDEX'}++;
          $loggerHash{ $app }->{'SIZE'} = 0;
       }
   }
   

   my $FH =  $loggerHash{ $app }->{'FH'};

   print $FH "[" . AbstractHandler::getCurrentDate() . "T" . AbstractHandler::getCurrentTime(). "] " . $2 ."\n";
   
   $loggerHash{ $app }->{'SIZE'} += length($2);
   
   $FH->flush();  

   $loggerSTDOUT->Info ( "APP [" . $1 . "] DATA [" . $2 . "]");
}

