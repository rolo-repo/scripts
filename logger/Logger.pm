package Logger;
#Description: Logger.pm
#			  Take care on output messages

use IO::File;
use File::Path;

#BEGIN {
#	if  ( eval { require Win32::Console::ANSI ; 1;} ) 
#	{
#		#module loaded
#		Win32::Console::ANSI->import();
#	}
#}
#
#use Term::ANSIColor;

my %g_logger_levels = (
						'DEBUG'    => 0,
						'TRACE'    => 1,
						'INFO'     => 2,
						'WARNING'  => 3,
						'ERROR'    => 4,
						'FATAL' => 5
);


$|=1;

sub new
{
	my ( $self, $level, $file ,$dir) = @_;
	my $data = {};
	$data->{LEVEL} =
	  ( defined $level ? $g_logger_levels{ uc($level) } : $g_logger_levels{'DEBUG'} );
	
	if ( defined $file )
	{
		my $origin_file_name = $file;
		my $index = 0;
		while (-e $file)
		{
			$file = (($index > 0) ? $index.'_'.$origin_file_name : $origin_file_name);
			$index++;
		}
		if( ! -e $dir)
		{
			mkpath(	$dir );
		}
		$data->{DEBUG_FILE} = new IO::File(">> $dir/$file");
	}
	bless $data, $self;
}

#sub set_level {
#	my ($self, $l) = @_;
#	$self->Error("Wrong logger level name : $l\n") if (! exists $g_logger_levels{$l});
#	$g_level=$g_logger_levels{$l};
#}
sub Debug()
{
	my ( $self, $msg ) = @_;
	$self->print_message( $msg, 'DEBUG' );
}

sub Trace()
{
	my ( $self, $msg ) = @_;
	$self->print_message( $msg, 'TRACE' );
}

sub Info()
{
	my ( $self, $msg ) = @_;
	$self->print_message( $msg, 'INFO' );
	return 1;
}

sub Warning()
{
	my ( $self, $msg ) = @_;
	$self->print_message( $msg, 'WARNING' );
}

sub Error()
{
	my ( $self, $msg ) = @_;
	$self->print_message( $msg, 'ERROR' );
	$!=undef;
	return undef;
}

sub Fatal()
{
	my ( $self, $msg ) = @_;
	$self->print_message( $msg, 'FATAL' );
	die "\n";
}

sub print_message($$$)
{
	my ( $self, $msg, $level_name ) = @_;
	if ( $g_logger_levels{$level_name} >= $self->{LEVEL} )
	{
		my $message = $self->_format_message( $msg, $level_name );
		if ( defined $self->{DEBUG_FILE} )
		{
			$self->_print_to_file( $message );			
		} 
		else
		{
			$self->_print_on_screen( $message );
		}
	}
}

sub _format_message($$$)
{
	my ( $self, $message_text, $level ) = @_;
	my $time = $self->_eval_time();

	chomp($message_text);
	
	#get the name of current function
	#the name is extracted from previuse frame the line id from the next one
	my $sub_name = (caller(3))[3] || 'main' ;
	$sub_name  =~ s/^.*:://;
	$sub_name = $sub_name . ":" . (caller(2))[2];
    
  chomp($sub_name);

	return '[' .$time .']' . '[' . $self->_colorize ( $level ) . ']' . '[' .$sub_name .'] ' . $message_text . "\n";
}

sub _colorize ($$)
{
	my ( $self , $level ) = @_;	
  
  #WA for some bug that takes a long time to load the ANSI colors DLL in windows
  return $level ;#if  ( ! eval { require Win32::Console::ANSI ; 1;} ) ;

	my $color;
	
	SWITCH:
	{
		if($g_logger_levels{ $level } == 0) {goto DEBUG;}
		if($g_logger_levels{ $level } == 1) {goto TRACE;}
		if($g_logger_levels{ $level } == 2) {goto INFO;}
		if($g_logger_levels{ $level } == 3) {goto WARNING;}
		if($g_logger_levels{ $level } == 4) {goto ERROR;}
		if($g_logger_levels{ $level } == 5) {goto FATAL;}
	
		DEBUG:
			goto INFO;
		TRACE:
			goto INFO;
		INFO:
			$color = color( "green" );
			last SWITCH;
		WARNING:
			$color = color( "yellow" );
			last SWITCH;
		ERROR:
			$color = color ( "red" );
			last SWITCH;
		FATAL:
			$color = color ( "bold red" );
			last SWITCH;		
	}
	
	return $color.$level.color("reset"); 
}

sub _print_on_screen($$)
{
	my ( $self, $message  ) = @_;
	
	print $message;
}

sub _print_to_file($$)
{
	my ( $self, $message ) = @_;
	my $FH = $self->{DEBUG_FILE};
	print $FH $message;
	$self->{DEBUG_FILE}->flush();
}

sub _eval_time()
{
	my ( $sec, $min, $hour, $mday, $mon, $year ) = localtime(time);
	$year += 1900;
	$mon++;
	$sec  = '0' . $sec  unless length($sec) > 1;
	$min  = '0' . $min  unless length($min) > 1;
	$hour = '0' . $hour unless length($hour) > 1;
	return $mday . '/' . $mon . '/' . $year . ' ' . $hour . ':' . $min . ':'
	  . $sec;
}


1;
