package pocci;

use DBI;

sub new
{
	my ( $self, $logger ) = @_;
	my $class = ref($self) || $self;
	
	return bless {'logger'=>$logger ,'user' => 'user_name' , 'pass' => 'password' , 'inst' => 'instance' , 'dbh' => undef}, $class;
}

sub open ( $$$ )
{
	my ( $self, $dbuser , $dbpassword , $dbinst  ) = @_;
	
	my $logger=$self->{'logger'};

	return if( defined $self->{'dbh'} );

	$self->{user} = $dbuser;
	$self->{pass} = $dbpassword;
	$self->{inst} = $dbinst;
		
	$logger->Info("Connecting to DB :" . $self->{user} ."/". $self->{pass} . "@" . $self->{inst} );

	my $dbh = DBI->connect('dbi:Oracle:'.$self->{inst}, $self->{user}, $self->{pass}, { RaiseError => 0, AutoCommit => 0 }) or return $logger->Error( "Failed to connect to $dbuser/$dbpassword@$dbinst");
	
	$self->{'dbh'} = $dbh;
	
	$dbh->do("ALTER SESSION SET NLS_DATE_FORMAT = 'MM/DD/YYYY HH:MI:SS AM'");
    $dbh->{LongTruncOk} =  1; 
	
	return 1;
}

sub DESTROY
{
    my $self = shift;
   	$self->{'dbh'}->disconnect();
	$self->{'dbh'} = undef;	
}

sub close()
{
	my $self = shift;
	
	my $logger=$self->{'logger'};
	
	return $logger->Error("DB handler not initialized call open method first") if( ! defined $self->{'dbh'} );
	
	$logger->Info("Closing connection to DB : ". $self->{user} ."/". $self->{pass} . "@" . $self->{inst});
	
	$self->{'dbh'}->disconnect();
	$self->{'dbh'} = undef;	
	
	return 1;
}

sub fetchData ($$)
{
	my ( $self , $query , $handler ) = @_;
	
	my $logger=$self->{'logger'};
	
	return $logger->Error("DB handler not initialized call open method first") if( ! defined $self->{'dbh'} );
	
	my $dbh = $self->{'dbh'};
	
	my $sth = $dbh->prepare($query);
	
	$sth->execute() or return $logger->Error( "Failed to execute the query [$query] ");
	
	while( $db_data = $sth->fetchrow_hashref() )
	{
		&$handler( $db_data ) 
			or return $logger->Error("Failed to handle data, $!");	
	}
	
	return 1;
}

sub runQuery ($)
{
   my ( $self , $query ) = @_;
 
   my $logger=$self->{'logger'};
   return $logger->Error("DB handler not initialized call open method first") if( ! defined $self->{'dbh'} );
  
   my $dbh = $self->{'dbh'};
   
   $logger->Debug("Running : $query");
   
   my $sth = $dbh->prepare($query);
    
   $sth->execute() or return $logger->Error( "Failed to execute the query [$query] ");
   
   return 1;
}

sub commit()
{
 my $self = shift;
 my $logger=$self->{'logger'};
 return $logger->Error("DB handler not initialized call open method first") if( ! defined $self->{'dbh'} );
 
 my $dbh = $self->{'dbh'};
 
 $logger->Debug ("Execute commit");
 $dbh->commit() or return $logger->Error( "Failed to execute commit ");;
 
 return 1; 
}

sub rollback()
{
 my $self = shift;
 my $logger=$self->{'logger'};
 return $logger->Error("DB handler not initialized call open method first") if( ! defined $self->{'dbh'} );
 
 my $dbh = $self->{'dbh'};
 
 $logger->Debug ("Execute rollback");
 $dbh->rollback() or return $logger->Error( "Failed to execute rollback ");;
 
 return 1; 
}
1;
