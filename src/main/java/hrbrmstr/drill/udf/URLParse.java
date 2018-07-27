package hrbrmstr.drill.udf;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

import io.mola.galimatias.*;
import io.mola.galimatias.canonicalize.*;

import javax.inject.Inject;

@FunctionTemplate(
  names = { "url_parse" },
  scope = FunctionTemplate.FunctionScope.SIMPLE,
  nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
)
public class URLParse implements DrillSimpleFunc {
  
  @Param VarCharHolder url_string;
  
  @Output BaseWriter.ComplexWriter out;
  
  @Inject DrillBuf buffer;
    
  public void setup() {}
  
  public void eval() {
    
    org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter mw = out.rootAsMap();

    String url = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
      url_string.start, url_string.end, url_string.buffer
    );

    try {

      io.mola.galimatias.URL curl = io.mola.galimatias.URL.parse(url);

      String scheme = curl.scheme();
      String username = curl.username();
      String password = curl.password();
      String host = curl.host().toString();
      String port = java.lang.Integer.toString(curl.port());
      String path = curl.path();
      String query = curl.query();
      String fragment = curl.fragment();

      org.apache.drill.exec.expr.holders.VarCharHolder row = new org.apache.drill.exec.expr.holders.VarCharHolder();

      if (scheme != null) {
        byte[] schemeBytes = scheme.getBytes();
        buffer.reallocIfNeeded(schemeBytes.length); 
        buffer.setBytes(0, schemeBytes);
        row.start = 0; 
        row.end = schemeBytes.length; 
        row.buffer = buffer;
        mw.varChar("scheme").write(row);
      }

      if (username != null) {
        byte[] userBytes = username.getBytes();
        buffer.reallocIfNeeded(userBytes.length); 
        buffer.setBytes(0, userBytes);
        row.start = 0; 
        row.end = userBytes.length; 
        row.buffer = buffer;
        mw.varChar("username").write(row);
      }

      if (password != null) {
        byte[] passBytes = password.getBytes();
        buffer.reallocIfNeeded(passBytes.length); 
        buffer.setBytes(0, passBytes);
        row.start = 0; 
        row.end = passBytes.length; 
        row.buffer = buffer;
        mw.varChar("password").write(row);
      }

      if (host != null) {
        byte[] hostBytes = host.getBytes();
        buffer.reallocIfNeeded(hostBytes.length); 
        buffer.setBytes(0, hostBytes);
        row.start = 0; 
        row.end = hostBytes.length; 
        row.buffer = buffer;
        mw.varChar("host").write(row);
      }

      if (port != null) {
        byte[] portBytes = port.getBytes();
        buffer.reallocIfNeeded(portBytes.length); 
        buffer.setBytes(0, portBytes);
        row.start = 0; 
        row.end = portBytes.length; 
        row.buffer = buffer;
        mw.varChar("port").write(row);
      }

      if (path != null) {
        byte[] pathBytes = path.getBytes();
        buffer.reallocIfNeeded(pathBytes.length); 
        buffer.setBytes(0, pathBytes);
        row.start = 0; 
        row.end = pathBytes.length; 
        row.buffer = buffer;
        mw.varChar("path").write(row);
      }

      if (query != null) {
        byte[] queryBytes = query.getBytes();
        buffer.reallocIfNeeded(queryBytes.length); 
        buffer.setBytes(0, queryBytes);
        row.start = 0; 
        row.end = queryBytes.length; 
        row.buffer = buffer;
        mw.varChar("query").write(row);
      }

      if (fragment != null) {
        byte[] fragBytes = fragment.getBytes();
        buffer.reallocIfNeeded(fragBytes.length); 
        buffer.setBytes(0, fragBytes);
        row.start = 0; 
        row.end = fragBytes.length; 
        row.buffer = buffer;
        mw.varChar("fragment").write(row);
      }

    } catch(Exception e) {
    }

  }
  
}
