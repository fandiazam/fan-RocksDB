/// <summary>
/// format the dec integer to a hex stream, for sending msg
/// </summary>
/// <param name="size"> dec integer </param>
/// <returns> hex stream </returns>
public string Dec2HexStream(int size)
{

    string raw = "00000000";
    string hex = Convert.ToString(size, 16);
    //Console.WriteLine("Conversion string" + hex + "length" + hex.Length);
    int hexLen = hex.Length;
    if (hexLen > 8)
        return "\x00\x00\x00\x00";
    else
    {
        // 100 -> 00 01 00 00 -> 00 00 01 00
        hex = raw.Substring(0, 8 - hexLen) + hex;
    }
    //Console.WriteLine("Add 0 string" + hex);

    string res = "";
    for (int i = 3; i >= 0; i--)
    {
        res += hex.Substring(i * 2, 2);
    }
    hex = res;

    //Console.WriteLine("Reverse string" + hex);
    res = "";
    for (int i = 0; i < 4; i++)
    {
        res += "\\x" + hex.Substring(i * 2, 2);
    }
    //Console.WriteLine("Result string" + res);
    return Regex.Unescape(res);
}

/// <summary>
/// for recving msg and parsering the size of data
/// </summary>
/// <param name="s"> hex stream </param>
/// <returns>dec integer</returns>
public int HexStream2Dec(string s)
{
    // 00 01 00 00
    // 00 00 01 00
    // \x01\x00\x00\x00 little endian
    UInt32 res = 0;
    //Console.WriteLine("Processing string" + s);
    string tmp = "";
    for (int i = 3; i >= 0; i--)
    {
        UInt32 m = Convert.ToUInt32(s[i]);
        string hexs = Convert.ToString(m, 16);
        tmp += hexs;
    }
    //Console.WriteLine("Restore String" + tmp + "Length" + (tmp.Length).ToString());
    res = Convert.ToUInt32(tmp, 16);
    return (int)res;
}