using System.Net;
using System.Net.Sockets;
using System.Text;


var ParseLength = (Byte[] bytes, int start) =>
{
  int len = 0;
  int i = start + 1;
  int consumed = 1;

  // 0x0D is '\r' hex value
  while (bytes[i] != 0x0D)
  {
    len = (len * 10) + (bytes[i] - '0');
    i++;
    consumed++;
  }

  // consumed + 2 to account for ending '\r\n'
  return (len, consumed + 2);
};

var ParseBulkStr = (Byte[] bytes, int start) =>
{
  var (len, consumed) = ParseLength(bytes, start);
  Byte[] payload = new Byte[len];
  Array.Copy(bytes, start + consumed, payload, 0, len);
  var parsed = Encoding.ASCII.GetString(payload);
  Console.WriteLine("ParsedBulkStr: {0}", parsed);
  return (parsed, consumed + len);
};

var ParseClientMsg = (Byte[] msg) =>
{
  // Console.WriteLine("Client msg: {0}", Encoding.ASCII.GetString(msg));

  var (bulkStrsToParse, cursor) = ParseLength(msg, 0);
  List<String> bulkStrs = new List<string>();

  for (int i = 0; i < bulkStrsToParse; i++)
  {
    var (str, consumed) = ParseBulkStr(msg, cursor);
    bulkStrs.Add(str);
    cursor += consumed;
  }

  return bulkStrs;
};

var ProcessPing = (String s) =>
{
  return s.ToLower() switch
  {
    "ping" => "+PONG\r\n",
    _ => String.Format("${0}\r\n{1}\r\n", s.Length - 5, s.Substring(5)),
  };
};

TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();

var HandleClient = (Socket client) =>
{
  while (true)
  {
    try
    {
      Byte[] buf = new Byte[client.SendBufferSize];
      var bytesRead = client.Receive(buf);
      if (bytesRead < buf.Length)
      {
        Array.Resize(ref buf, bytesRead);
      }

      List<string> commands = ParseClientMsg(buf);
      foreach (string command in commands)
      {
        Console.WriteLine("Executing command: {0}", command);
        var response = command.ToLower() switch
        {
          var s when s.StartsWith("ping") => ProcessPing(s),
          _ => throw new ArgumentException(String.Format("Received unknown redis command: {0}", command)),
        };
        client.Send(Encoding.ASCII.GetBytes(response));
      }
    }
    catch (Exception e)
    {
      Console.WriteLine("Caught exception: {0}", e.ToString());
    }
  }
};

while (true)
{
  try
  {
    Socket client = server.AcceptSocket(); // wait for client
    new Thread(() => HandleClient(client)).Start();
  }
  catch (Exception e)
  {
    Console.WriteLine("Caught exception: {0}", e.ToString());
  }
}
