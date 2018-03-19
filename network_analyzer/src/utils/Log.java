package utils;

import java.io.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Log
{
    public Log()
    {
        streamList = new ArrayList<PrintStream>();
    }

    public PrintStream add(OutputStream stream)
    {
        PrintStream printStream = new PrintStream(stream);

        streamList.add(printStream);

        return printStream;
    }

    public PrintStream add(File file) throws FileNotFoundException
    {
        return add(new FileOutputStream(file));
    }

    public void remove(PrintStream stream)
    {
        streamList.remove(stream);

        if(stream != System.out)
        {
            stream.close();
        }
    }

    public void close()
    {
        for(PrintStream stream : streamList)
        {
            if(stream != System.out)
            {
                stream.close();
            }
        }
    }

    public void info(String msg)
    {
        print(msg);
    }

    public void error(Exception e)
    {
        String msg = e instanceof RuntimeException ?
                e.toString() : e.getMessage();

        error(msg);
    }

    public void error(String msg)
    {
        print("ERROR: " + msg);
    }

    private void print(String msg)
    {
        String time = FORMAT.format(new Date());

        msg = time + ": " + msg;

        for(PrintStream stream : streamList)
        {
            stream.println(msg);
            stream.flush();
        }
    }

    private static final SimpleDateFormat FORMAT =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // PrintStream is thread safe. No synchronization is required.
    private final List<PrintStream> streamList;
}
