#!/usr/local/bin/perl
#ver 2
BEGIN {	
	push @INC, "C:/PERLMODULES/", $ENV{PERLMODULES} ,"$ENV{HOME}//PERLMODULES/" , "c:/Perl/site/lib/";
}

use DBI;
my $dbh;

$USER='USCDB60';
$PASS='USCDB60';
$INST='USCABPD2';


sub parse{
my $value=$_[0];
if(!defined $$value){
	$$value="NULL";
	return;
	}			
if($$value =~ /(\d){2}\/(\d){2}\/(\d){4} (\d){2}:(\d){2}:(\d){2} (AM|PM)/){
	$$value="TO_DATE(".$dbh->quote($$value).",'MM/DD/YYYY HH:MI:SS AM')";
	return;
	}
if(($$value =~ /[a-zA-Z_\~\!\@\#\$%^&*()\-=,.?|\{\}\[\]\s\D]/)){
	$$value=$dbh->quote($$value);
	return;
	}
}
 
sub generate_insert_statment{

my ($table_name,@conditions)=@_;
my $query = 'select * from ' . $table_name ;
	if($#conditions >= 0) {
		$query .= ' where ';
		foreach (@conditions) {
			$query .= $_ . " ";		
		}
	}
$dbh = DBI->connect('dbi:Oracle:'.$INST, $USER, $PASS) or die "Error";
$dbh->do("ALTER SESSION SET NLS_DATE_FORMAT = 'MM/DD/YYYY HH:MI:SS AM'");
$dbh->{LongTruncOk} =  1;

my $sth = $dbh->prepare($query);

my $rc = $sth->execute;
my $db_data;
my @table;
while($db_data=$sth->fetchrow_hashref){
#my @av_keys=keys(%{$db_data});
	push(@table,$db_data);
}

$sth->finish;

$dbh->disconnect();
my @statments;

foreach $row (@table){
	foreach $elem (keys(%{$row})){
		#%{$db_data}->{$_}=parse(\%{$db_data}->{$_});
		parse(\%{$row}->{$elem});
		}
 push(@statments,'INSERT INTO '.$table_name.'('.join(',',keys(%{$row})).') VALUES ('.join(',',values(%{$row})).');');
}

return @statments;
}


open(FILE,">Inserts.sql") or die ("Failed to open file <Inserts.sql>");
#open (AVAILABLE_TABLES,"<tables.txt") or die ("Failed to open file <tables.txt>");
#my @available_tables =<AVAILABLE_TABLES>;
my @available_tables=@ARGV;
foreach(@available_tables)
{
	print FILE "delete from $_;\n";
	my @statment = generate_insert_statment(uc($_));

	foreach $line (@statment)
	{
		print FILE "$line\n";
	}
	print FILE "COMMIT;\n";
}

print FILE "exit;";

close(FILE);
close(AVAILABLE_TABLES);
