using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamRepository.Azure
{
    public struct Position
    {
        public readonly static Position Start = new Position(0);

        private int _page;
        public int Page
        {
            get { return _page; }
        }
        private int _offset;
        public int Offset
        {
            get { return _offset; }
        }


        public Position(int page, int offset)
        {
            _page = page;
            _offset = offset;
        }

        public Position(long length)
        {
            _page = Math.DivRem((int)length, PageBlobState.PageSize, out _offset);
        }


        public int ToLinearAddress()
        {
            return Page * PageBlobState.PageSize + Offset;
        }

        //public int ToPageAddress()
        //{
        //    return Page * PageBlobState.PageSize;
        //}

        public static Position operator +(Position position, int increment)
        {
            return new Position(position.ToLinearAddress() + increment);
        }


        public override string ToString()
        {
            return string.Format("Page {0}, Offset {1}.   Linear {2}", Page, Offset, ToLinearAddress());
        }
    }
}
