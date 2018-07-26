package hrbrmstr.drill.udf;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

//import io.mola.galimatias.*;
//import io.mola.galimatias.canonicalize.*;

import javax.inject.Inject;

@FunctionTemplate(
  names = { "url_parse" },
  scope = FunctionTemplate.FunctionScope.SIMPLE,
  nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
)
public class URLParse implements DrillSimpleFunc {
  
  @Param NullableVarCharHolder url_string;
  
  @Output BaseWriter.ComplexWriter out;
  
  @Inject DrillBuf buffer;
    
  public void setup() {}
  
  public void eval() {
    
    org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter mw = out.rootAsMap();

    String url = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
      url_string.start, url_string.end, url_string.buffer
    );
    
    if (url.isEmpty() || url == null) url = "";

    String scheme = null;
    String username = null;
    String password = null;
    String host = null;
    String port = null;
    String path = null;
    String query = null;
    String fragment = null;
    String errormsg = null;

    url = url.toLowerCase();
    url = url.trim();

    if (url.length() > 0) {
      
      try {
        io.mola.galimatias.URL curl = io.mola.galimatias.URL.parse(url);

        scheme = curl.scheme();
        username = curl.username();
        password = curl.password();
        host = curl.host().toString();
        port = java.lang.Integer.toString(curl.port());
        path = curl.path();
        query = curl.query();
        fragment = curl.fragment();
    
      } catch(io.mola.galimatias.GalimatiasParseException e) {
        errormsg = e.getMessage();
      }

    }

    org.apache.drill.exec.expr.holders.VarCharHolder row;
    byte[] outBytes;

    if (scheme != null) {
      row = new org.apache.drill.exec.expr.holders.VarCharHolder();
      outBytes = scheme.getBytes();
      buffer.reallocIfNeeded(outBytes.length); 
      buffer.setBytes(0, outBytes);
      row.start = 0; 
      row.end = outBytes.length; 
      row.buffer = buffer;
      mw.varChar("scheme").write(row);
    }

    if (username != null) {
      row = new org.apache.drill.exec.expr.holders.VarCharHolder();
      outBytes = username.getBytes();
      buffer.reallocIfNeeded(outBytes.length); 
      buffer.setBytes(0, outBytes);
      row.start = 0; 
      row.end = outBytes.length; 
      row.buffer = buffer;
      mw.varChar("username").write(row);
    }

    if (password != null) {
      row = new org.apache.drill.exec.expr.holders.VarCharHolder();
      outBytes = password.getBytes();
      buffer.reallocIfNeeded(outBytes.length); 
      buffer.setBytes(0, outBytes);
      row.start = 0; 
      row.end = outBytes.length; 
      row.buffer = buffer;
      mw.varChar("password").write(row);
    }

    if (host != null) {
      row = new org.apache.drill.exec.expr.holders.VarCharHolder();
      outBytes = host.getBytes();
      buffer.reallocIfNeeded(outBytes.length); 
      buffer.setBytes(0, outBytes);
      row.start = 0; 
      row.end = outBytes.length; 
      row.buffer = buffer;
      mw.varChar("host").write(row);
    }

    if (port != null) {
      row = new org.apache.drill.exec.expr.holders.VarCharHolder();
      outBytes = port.getBytes();
      buffer.reallocIfNeeded(outBytes.length); 
      buffer.setBytes(0, outBytes);
      row.start = 0; 
      row.end = outBytes.length; 
      row.buffer = buffer;
      mw.varChar("port").write(row);
    }

    if (path != null) {
      row = new org.apache.drill.exec.expr.holders.VarCharHolder();
      outBytes = path.getBytes();
      buffer.reallocIfNeeded(outBytes.length); 
      buffer.setBytes(0, outBytes);
      row.start = 0; 
      row.end = outBytes.length; 
      row.buffer = buffer;
      mw.varChar("path").write(row);
    }

    if (query != null) {
      row = new org.apache.drill.exec.expr.holders.VarCharHolder();
      outBytes = query.getBytes();
      buffer.reallocIfNeeded(outBytes.length); 
      buffer.setBytes(0, outBytes);
      row.start = 0; 
      row.end = outBytes.length; 
      row.buffer = buffer;
      mw.varChar("query").write(row);
    }

    if (fragment != null) {
      row = new org.apache.drill.exec.expr.holders.VarCharHolder();
      outBytes = fragment.getBytes();
      buffer.reallocIfNeeded(outBytes.length); 
      buffer.setBytes(0, outBytes);
      row.start = 0; 
      row.end = outBytes.length; 
      row.buffer = buffer;
      mw.varChar("fragment").write(row);
    }

    if (errormsg != null) {
      row = new org.apache.drill.exec.expr.holders.VarCharHolder();
      outBytes = errormsg.getBytes();
      buffer.reallocIfNeeded(outBytes.length); 
      buffer.setBytes(0, outBytes);
      row.start = 0; 
      row.end = outBytes.length; 
      row.buffer = buffer;
      mw.varChar("errormsg").write(row);
    }
    
  }
  
}
