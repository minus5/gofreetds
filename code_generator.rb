require 'erb'

#usage:
# ruby code_generator.rb | pbcopy

template  = 
"<%data.each do |type|%>
    case *<%=type%>:
			*f, ok = rValue.(<%=type%>)
			if !ok {
				return errors.New(fmt.Sprintf(\"Failed to convert %T to <%=type%>.\", rValue))
			}<%end%>"

data = %w(string int uint8 int16 int32 int64 float32 float64 bool []byte time.Time)

print ERB.new(template).result
print "\n\n"
