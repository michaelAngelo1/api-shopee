export function formatUnixTime(origin, unixTimestamp) {
  const date = new Date(unixTimestamp * 1000);
  const options = {
    timeZone: 'Asia/Jakarta',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23' 
  };

  return new Intl.DateTimeFormat('sv-SE', options).format(date);
}